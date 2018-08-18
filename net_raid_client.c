/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include <dirent.h>

// static const char *hello_str = "Hello World!\n";
// static const char *hello_path = "/hello";


enum func{READDIR, GETATTR, OPEN, READ, MKDIR, RMDIR, UNLINK, SYMLINK, RELEASE, WRITE, OPENDIR, RELEASEDIR, RENAME, CREATE, TRUNCATE};
enum file_type{ISREG, ISDIR, ISCHR, ISBLK, ISFIFO, ISLNK, ISSOCK, NOFILE};  //N_FILE = NO FILE EXISTED, DIR = DIRECTORY, R_FILE = REGULAR FILE

pthread_mutex_t lock;

char * log_path;
int cacheSize;
char * replace;
int timeout;

int nMounts;

int CHUNK_SIZE = 512;

int sockfd_1;
int sockfd_2;

struct serv_addr{
	char * ip;
	int port;
};

struct mount{
	char * diskname;
	char * mountpoint;
	int raid_id;
	struct serv_addr ** servers;
	int nServers;
	char * hotswap;
};

struct c_file{
	char * name;
	char * path;
	int flags;
	int fd;
	enum file_type ft;
};

struct mount ** mounts;



int putInt(char * buffer, int num, int offset){
    uint32_t tmp;
    tmp = htonl(num);
    memcpy(buffer + offset, (char*)&tmp, 8);
    return 8;
}

int readInt(int connfd){
    char tmp[8];
    recv(connfd, tmp, 8, 0);
    uint32_t raw = *(uint32_t*)tmp;
    return ntohl(raw);
}

int getSockfd(int serv_index){
	char * ip = mounts[0]->servers[serv_index]->ip;
	int port = mounts[0]->servers[serv_index]->port;
	
	// int err = 0; 
	if(sockfd_1 == -1){
		struct sockaddr_in serv_addr; 
		if((sockfd_1 = socket(AF_INET, SOCK_STREAM, 0)) < 0){
	        return -1;
	    } 
		memset(&serv_addr, '0', sizeof(serv_addr)); 
		serv_addr.sin_family = AF_INET;
	    serv_addr.sin_port = htons(port); 
		if(inet_pton(AF_INET, ip, &serv_addr.sin_addr)<=0){
	        return -1;
	    }

		if(connect(sockfd_1, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
	       return -1;
	    } 
	}
    return sockfd_1;
}

int sendData(int sockfd, char * buffer, int size){
	int sent = 0;
    while(sent < size){
		int bytes = write(sockfd,buffer + sent, size - sent);
        if (bytes < 0)
            break;
        sent += bytes;
	} 
	return sent;
}

static int client_getattr(const char *path, struct stat *stbuf)
{
	pthread_mutex_lock(&lock);
	printf("CLIENT_GETATTR %s\n", path);
	int sockfd, total = 0, sent = 0, err, path_len; 
	char sendBuffer[1024];
    
    // return 0;
	if(strlen(path) == 0){
    	pthread_mutex_unlock(&lock);
		return 0;
	}
	sockfd = getSockfd(0);

    path_len = strlen(path);
    total += putInt(sendBuffer, GETATTR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;

    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
    
	err = readInt(sockfd);
	if (err == 0){
		memset(stbuf, 0, sizeof(struct stat));
		//stbuf->st_dev = 0; // ignored
		//stbuf->st_ino = 0; // ignored for now
		stbuf->st_mode = (mode_t) readInt(sockfd);
		stbuf->st_nlink = (nlink_t) readInt(sockfd);
		stbuf->st_uid = (uid_t) readInt(sockfd);
		stbuf->st_gid = (gid_t) readInt(sockfd);
		stbuf->st_rdev = (dev_t) readInt(sockfd);
		//stbuf->st_blksize = 0; // ignored
		stbuf->st_blocks = (blkcnt_t) readInt(sockfd);
		stbuf->st_size = (off_t) readInt(sockfd);
		stbuf->st_atime = time( NULL );
		stbuf->st_mtime = time( NULL );
		stbuf->st_ctime = time( NULL );
	}
	pthread_mutex_unlock(&lock);
	return err;
}

static int client_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	pthread_mutex_lock(&lock);
	printf("CLIENT_READDIR %s\n", path);
	(void) offset;
	(void) fi;
	int sockfd, path_len, total = 0, err, sent = 0, fNum, i, nameLen; 
	char sendBuffer[1024];
    sockfd = getSockfd(0);
    
    path_len = strlen(path);
    total += putInt(sendBuffer, READDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;

    err = readInt(sockfd);
    if(err == 0){
    	fNum = readInt(sockfd);
    	for(i = 0; i < fNum; i++){
	        struct stat st;
			memset(&st, 0, sizeof(st));
			
			nameLen = readInt(sockfd);
	        char name [nameLen + 1];
	        read(sockfd, &name, nameLen);
	        name[nameLen] = '\0';

	        st.st_ino = readInt(sockfd);
			st.st_mode = readInt(sockfd);
			
			if (filler(buf, name, &st, 0))
				break;
		}
    }
    pthread_mutex_unlock(&lock);
    return err;
}

static int client_open(const char *path, struct fuse_file_info *fi)
{	
	pthread_mutex_lock(&lock);
	printf("CLIENT_OPEN %s\n", path);
	struct c_file *f = malloc(sizeof(struct c_file));
    memset(f, 0, sizeof(struct c_file));
    int flags, sockfd, path_len, total = 0, sent = 0, err;
	char sendBuffer[1024];
    
    flags = fi->flags;
	sockfd = getSockfd(0);
    
	path_len = strlen(path);
    total += putInt(sendBuffer, OPEN, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, flags, total);
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	err = readInt(sockfd);
	if(err == 0){
		// f->path = malloc(strlen(path) + 1);
		// strcpy(f->path, path);
		f->flags = flags;
		f->fd = readInt(sockfd);
		printf("OPEN_END: RETURNED FILE DESCRIPTOR - %d\n", f->fd);
		fi->fh = (long)f;
	}else{
		printf("OPEN_END: COULD NOT OPEN FILE. SERVER RESPONDED: %d\n", err);
	}
	pthread_mutex_unlock(&lock);
	return err;
}

static int client_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	pthread_mutex_lock(&lock);
	printf("CLIENT_READ %s\n", path);
	struct c_file *f = (struct c_file *)fi->fh;
	int sockfd, fd, total = 0, err, sent = 0, n_bytes = 0, path_len;
	char sendBuffer[1024];
    if(f != NULL)
		fd = f->fd;
	else
		fd = -1;

	sockfd = getSockfd(0);
	
	path_len = strlen(path);
	total += putInt(sendBuffer, READ, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, fd, total);
    total += putInt(sendBuffer, size, total);
    total += putInt(sendBuffer, offset, total);
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
    
	err = readInt(sockfd);
	if(err != 0){
    	pthread_mutex_unlock(&lock);
		return err;	
	}
	n_bytes = readInt(sockfd);
	printf("****bytes read %d ******\n", n_bytes);
	// char content[n_bytes + 1];
	// if(n_bytes > 0){
	read(sockfd, buf, n_bytes);
    // content[n_bytes] = '\0';
    // strcpy(buf, content);
	// }
	printf("READ_END: BYTES READ: %d\n", n_bytes);
    // memcpy(buf, content, n_bytes);
    pthread_mutex_unlock(&lock);
	return n_bytes;
}


//As for read above, except that it can't return 0.
static int client_write(const char* path, const char *buff, size_t size, off_t offset, struct fuse_file_info* fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_WRITE %s\n", path);
	
	struct c_file *f = (struct c_file *)fi->fh;
	int fd, path_len = strlen(path), buf_len = strlen(buff), sent = 0, err, send_size, total = 0, b_written;
    if(f != NULL)
		fd = f->fd;
	else
		fd = -1;

	printf("WRITE: FILE DESCRIPTOR - %d\n", fd);
    
    int sockfd = getSockfd(0);
	int total_written = 0;

	char sendBuffer[4096];
	int buf_offset = 0;
	printf("Size of buffer: %d, Total number of bytes to send: %d\n", (int)buf_len, (int)size);
	total += putInt(sendBuffer, WRITE, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, fd, total);
	
	total += putInt(sendBuffer, size, total);
	total += putInt(sendBuffer, offset, total);
    sent = sendData(sockfd, sendBuffer, total);
	
	while(size > 0){
		total = 0;
	    send_size = size;
		if(send_size > CHUNK_SIZE)
			send_size = CHUNK_SIZE;
		printf("Trying to send %d bytes over socket. bytes sent so far: %d \n", send_size, buf_offset);
		total += putInt(sendBuffer, send_size, total);
	    memcpy(sendBuffer + total, buff + buf_offset, send_size);
	    total += send_size;

		sent = sendData(sockfd, sendBuffer, total);

	    err = readInt(sockfd);
		if(err != 0){
    		pthread_mutex_unlock(&lock);
			return err;	
		}
		
		
		b_written = readInt(sockfd);
    	total_written += b_written;
    	if(b_written < send_size){
			break;    		
    	}
		size -= send_size;
    	offset += send_size;
    	buf_offset += send_size;
    }
    
    (void) sent;
	printf("WRITE_END\n\n");
    pthread_mutex_unlock(&lock);
	return total_written;
}

//his is the only FUSE function that doesn't have a directly corresponding system call, although close(2) is related.
//Release is called when FUSE is completely done with a file; at that point, you can free up any temporarily allocated data structures. 
//The IBM document claims that there is exactly one release per open, but I don't know if that is true.
static int client_release(const char* path, struct fuse_file_info *fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RELEASE %s\n", path);
	struct c_file *f = (struct c_file *)fi->fh;
	int sockfd, err, total = 0, sent = 0;
	char sendBuffer[1024];
    if(f == NULL){
    	pthread_mutex_unlock(&lock);
		return -EACCES;
	}
	if(f->fd <= 0){
		pthread_mutex_unlock(&lock);
		return -EACCES;
	}
	sockfd = getSockfd(0);
	
	total += putInt(sendBuffer, RELEASE, total);
    total += putInt(sendBuffer, f->fd, total);
    (void) path;
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;

    err = readInt(sockfd);
    pthread_mutex_unlock(&lock);
    return err;
}

//Rename the file, directory, or other object "from" to the target "to". 
//Note that the source and target don't have to be in the same directory, so it may be necessary to move the source to an entirely new directory. 
//See rename(2) for full details.
static int client_rename(const char* from, const char* to){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RENAME %s  %s\n", from, to);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, from_len = strlen(from), to_len = strlen(to), err;
    total += putInt(sendBuffer, RENAME, total);
    total += putInt(sendBuffer, from_len, total);
    strcpy(sendBuffer + total, from);
    total += from_len;
    total += putInt(sendBuffer, to_len, total);
    strcpy(sendBuffer + total, to);
    total += to_len;

    // total += putInt(sendBuffer, (int)mode, total);
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

//Remove (delete) the given file, symbolic link, hard link, or special node. 
//Note that if you support hard links, unlink only deletes the data when the last hard link is removed. 
//See unlink(2) for details.
static int client_unlink(const char* path){
	pthread_mutex_lock(&lock);
	printf("CLIENT_UNLINK %s\n", path);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, path_len = strlen(path), err;
    total += putInt(sendBuffer, UNLINK, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

//Remove the given directory. This should succeed only if the directory is empty (except for "." and ".."). See rmdir(2) for details.
static int client_rmdir(const char* path){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RMDIR %s\n", path);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, path_len = strlen(path), err;
    total += putInt(sendBuffer, RMDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

//Create a directory with the given name. The directory permissions are encoded in mode. 
static int client_mkdir(const char* path, mode_t mode){
	pthread_mutex_lock(&lock);
	printf("CLIENT_MKDIR %s\n", path);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, path_len = strlen(path), err;
    total += putInt(sendBuffer, MKDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, (int)mode, total);
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

static int client_symlink(const char* from, const char * to){
	pthread_mutex_lock(&lock);
	printf("CLIENT_SYMLINK %s %s\n", from, to);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, path_len = strlen(from), path_len1 = strlen(to), err;
    total += putInt(sendBuffer, SYMLINK, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, from);
    total += path_len;
    total += putInt(sendBuffer, path_len1, total);
    strcpy(sendBuffer + total, to);
    total += path_len1;

    // total += putInt(sendBuffer, (int)mode, total);
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

/*  Open a directory for reading. */
static int client_opendir(const char* path, struct fuse_file_info* fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_OPENDIR %s\n", path);
	struct c_file *f = malloc(sizeof(struct c_file));
    memset(f, 0, sizeof(struct c_file));
	int sockfd, sent, err, total = 0, path_len = strlen(path);
	char sendBuffer[1024];
    
	sockfd = getSockfd(0);
	
	total += putInt(sendBuffer, OPENDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;
	
    err = readInt(sockfd);
    if(err == 0){
		f->path = malloc(strlen(path) + 1);
		strcpy(f->path, path);
		f->fd = readInt(sockfd);
		fi->fh = (long)f;
	}
	pthread_mutex_unlock(&lock);
	return err;
}

//This is like release, except for directories.
static int client_releasedir(const char* path, struct fuse_file_info *fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RELEASEDIR %s\n", path);
	struct c_file *f = (struct c_file *)fi->fh;
	int sockfd, total = 0, path_len = strlen(path), sent = 0, err;
	char sendBuffer[1024];
    if(f == NULL){
    	pthread_mutex_unlock(&lock);
		return -EACCES;
	}
	if(f->fd <= 0){
		pthread_mutex_unlock(&lock);
		return -EACCES;
	}
	sockfd = getSockfd(0);
	
	total += putInt(sendBuffer, RELEASEDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, f->fd, total);
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;

	err = readInt(sockfd);
	pthread_mutex_unlock(&lock);
    return err;
}

static int client_utimens(const char*path, const struct timespec ts[2]){
    return 0;
}


static int client_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	pthread_mutex_lock(&lock);
	printf("CLIENT_CREATE %s\n", path);
	int sockfd = getSockfd(0);
	
	char sendBuffer[1024];
    int total = 0, err, path_len = strlen(path);
    total += putInt(sendBuffer, CREATE, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, (int)mode, total);
    total += putInt(sendBuffer, (int)fi->flags, total);
    
    int sent = sendData(sockfd, sendBuffer, total);
    (void) sent;

    err = readInt(sockfd);
    if(err != 0){
    	pthread_mutex_unlock(&lock);
    	return err;
    }
	
    err = readInt(sockfd);
    pthread_mutex_unlock(&lock);
	return 0;
}

static int client_truncate(const char *path, off_t size, struct fuse_file_info *fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_TRUNCATE %s\n", path);
	struct c_file *f = NULL;// = (struct c_file *)fi->fh;
	int sockfd, total = 0, path_len = strlen(path), sent = 0, err, tfd = -1;
	char sendBuffer[1024];
    printf("aq modis? 1\n");

    if(f == NULL){
		tfd = -1;
	}else if(f->fd <= 0){
		tfd = -1;
	}else{
		tfd = f->fd;
	}
	sockfd = getSockfd(0);
	printf("aq modis? 2   %d\n", tfd);

    total += putInt(sendBuffer, TRUNCATE, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, tfd, total);
    total += putInt(sendBuffer, (int)size, total);
    
    sent = sendData(sockfd, sendBuffer, total);
    (void) sent;

    err = readInt(sockfd);
    if(err != 0){
    	pthread_mutex_unlock(&lock);
    	return err;
    }
    
    err = readInt(sockfd);
    pthread_mutex_unlock(&lock);
	return err;	
}



static struct fuse_operations client_oper = {
	.getattr	= client_getattr,
	.readdir	= client_readdir,
	.open		= client_open,
	.read		= client_read, 
	.write		= client_write,
	.release 	= client_release,
	.rename		= client_rename,
	.unlink 	= client_unlink,
	.rmdir 		= client_rmdir,
	.mkdir		= client_mkdir,
	// .opendir	= client_opendir,
	// .releasedir	= client_releasedir,
	.symlink 	= client_symlink,
	.create		= client_create,
	.truncate   = client_truncate,
	.utimens 	= client_utimens,
};





int readLine(char * buffer, int fd){
	char c;
    read(fd, &c, 1);
    int bytes = 0;
    while ((c != '\n') && (c != EOF)) {
	    buffer[bytes] = c;
	    bytes ++;
	    read(fd, &c, 1);
	}
	buffer[bytes] = '\0';
	return bytes;
}


char * getValue(char * line){
	char delim[1];
	delim[0] = ' ';
	strtok(line, delim);
	strtok(0, delim);
	char * tok = strtok(0, delim);
	char * ans = malloc(strlen(tok) + 1);
	strcpy(ans, tok);
	return ans;
}

int getServers(char * line, struct mount * mnt){
	char delim[4];
	delim[0] = ' ';
	delim[1] = '=';
	delim[2] = ',';
	delim[3] = ':';
	strtok(line, delim);
	char * tok;
	mnt->servers = malloc(32 * sizeof(struct serv_addr *));
	int nServ = 0;
	while((tok = strtok(0, delim)) != NULL){
		struct serv_addr * s_addr = malloc(sizeof(struct serv_addr));
		s_addr->ip = malloc(strlen(tok) + 1);
		strcpy(s_addr->ip,tok);
		tok = strtok(0, delim);
		s_addr->port = strtol(tok, (char **)NULL, 10);
		mnt->servers[nServ] = s_addr;
		nServ ++;
		// printf("nserv : %d\n", nServ);
	}
	
	return nServ;
}


void printcf(){
	int i;
	for(i = 0; i < nMounts; i++){
		printf("%s, %s, %d, %d\n", mounts[i]->diskname, mounts[i]->mountpoint, mounts[i]->raid_id, mounts[i]->nServers);
		printf("servers:\n");
		int j;
		for(j = 0; j < mounts[i]->nServers; j++){
			printf("ip: - %s, port: - %d\n", mounts[i]->servers[j]->ip, mounts[i]->servers[j]->port);
		}
		printf("\n");
	}
}

/* https://stackoverflow.com/questions/3501338/c-read-file-line-by-line */
int parsecf(char * path){
	int fd = open(path, O_RDONLY);
	if(fd < 0){
        return 1;
	}
	
	int maximumLineLength = 256;
	char *lineBuffer = (char *)malloc(sizeof(char) * maximumLineLength);
	if (lineBuffer == NULL) {
    	printf("Error allocating memory for line buffer.");
    	exit(1);
	}	
	
	nMounts = 0;
	
	int bytes = readLine(lineBuffer, fd);
	log_path = getValue(lineBuffer);
	
	bytes = readLine(lineBuffer, fd);
	cacheSize = strtol(getValue(lineBuffer), (char **)NULL, 10);
	
	bytes = readLine(lineBuffer, fd);
	replace = getValue(lineBuffer);
	
	bytes = readLine(lineBuffer, fd);
	timeout = strtol(getValue(lineBuffer), (char **)NULL, 10);
	
	bytes = readLine(lineBuffer, fd);
	bytes = readLine(lineBuffer, fd);

	mounts = (struct mount**)malloc(32 * sizeof(struct mount));
	
	while(bytes != 0){
		struct mount * mnt = malloc(sizeof(struct mount));
		mnt->diskname =	getValue(lineBuffer);
		readLine(lineBuffer, fd);
		mnt->mountpoint = getValue(lineBuffer);
		readLine(lineBuffer, fd);
		mnt->raid_id = strtol(getValue(lineBuffer), (char **)NULL, 10);
		readLine(lineBuffer, fd);
		mnt->nServers = getServers(lineBuffer, mnt);
		readLine(lineBuffer, fd);
		mounts[nMounts] = mnt;
		nMounts ++;

		bytes = readLine(lineBuffer, fd);
		if(bytes == 0){
			bytes = readLine(lineBuffer, fd);
		}
	}

	printcf();


	return 0;
}	


int main(int argc, char *argv[])
{
	if(argc == 1){
		printf("CONFIG: no config file specified. exitting");
		return 0;
	}
	char * path = malloc(strlen(argv[1]) + 1);
	strcpy(path, argv[1]);
	// printf("%s", path);
	if(parsecf(path) != 0){
		printf("CONFIG: bad formatting of config file. exitting");
		return 0;
	}
	sockfd_1 = -1;
	sockfd_2 = -1;
	if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init failed\n");
        return 1;
    }
	printf("connecting to ip: %s   port: %d \n", mounts[0]->servers[0]->ip, mounts[0]->servers[0]->port);
	// return 0;
	argv[1] = mounts[0]->mountpoint;
	printf("argv1 : %s\n", argv[1]);
	return fuse_main(argc, argv, &client_oper, NULL);
}

