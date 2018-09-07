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
#include <math.h>


#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>

#include <dirent.h>

#define MAX(x, y) (((x) > (y)) ? (x) : (y))
#define MIN(x, y) (((x) < (y)) ? (x) : (y))

// static const char *hello_str = "Hello World!\n";
// static const char *hello_path = "/hello";


enum func{READDIR, GETATTR, OPEN, READ, MKDIR, RMDIR, UNLINK, SYMLINK, RELEASE, WRITE, OPENDIR, RELEASEDIR, RENAME, CREATE, TRUNCATE};
enum file_type{ISREG, ISDIR, ISCHR, ISBLK, ISFIFO, ISLNK, ISSOCK, NOFILE};  //N_FILE = NO FILE EXISTED, DIR = DIRECTORY, R_FILE = REGULAR FILE
enum file_system{RAID_1, RAID_5};
enum s_error {CONNECTION_ERR, FUNCTION_ERR};

enum s_error SERV_ERR;
int my_errno;

pthread_mutex_t lock;

char * log_path;
int cacheSize;
char * replace;
int timeout;

int nMounts;

int CHUNK_SIZE = 512;
int BLOCK_SIZE = 7;

int sockfd_1;
int sockfd_2;

enum file_system f_s;


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
	int * fd_arr;
	enum file_type ft;
};


int n_servers;
int * s_fd_arr = NULL;
int n_servers_down;
int * d_servers = NULL;


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

int mark_down(int serv_index){
	if(SERV_ERR == CONNECTION_ERR){
		if(d_servers[serv_index] == 0){
			d_servers[serv_index] = -1;
			n_servers_down ++;
			if(n_servers_down > 1){
				printf("NUMBER OF SERVERS DOWN = %d, last server to went down: %d, exitting\n", n_servers_down, serv_index);
				exit(-1);
			}
		}
		return 0;
	}else if(SERV_ERR == FUNCTION_ERR){
		return -1;
	}else{
		printf("Unknown bug in program, please find your ruber duck, whatever u call it, to help you put yourself togather, to figure out what went down in this FUCKIN PROGRAMM!!!\n");
		return 0;
	}
}

int * getSockfd_arr(){
	int serv_index;
	char * ip;
	int port;
	if(s_fd_arr == NULL){
		s_fd_arr = malloc(sizeof(int) * n_servers);
		for(serv_index = 0; serv_index < n_servers; serv_index++)
			s_fd_arr[serv_index] = -1;
		
	}
	if(d_servers == NULL){
		d_servers = malloc(sizeof(int) * n_servers);
		for(serv_index = 0; serv_index < n_servers; serv_index++)
			d_servers[serv_index] = 0;
	}

	for(serv_index = 0; serv_index < n_servers; serv_index++){
		if(s_fd_arr[serv_index] == -1){
			ip = mounts[1]->servers[serv_index]->ip;
			port = mounts[1]->servers[serv_index]->port;
				
			struct sockaddr_in serv_addr; 
			if((s_fd_arr[serv_index] = socket(AF_INET, SOCK_STREAM, 0)) < 0){
		        continue;
		    } 
			memset(&serv_addr, '0', sizeof(serv_addr)); 
			serv_addr.sin_family = AF_INET;
		    serv_addr.sin_port = htons(port); 
			if(inet_pton(AF_INET, ip, &serv_addr.sin_addr)<=0){
		        continue;
		    }

			if(connect(s_fd_arr[serv_index], (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
		       continue;
		    }
		    printf("CONNECTION TO SERVER: %d WAS ESTABLISHED.\n", serv_index);
		}
	}
	// if(sockfd_1 == -1){
	// 	struct sockaddr_in serv_addr; 
	// 	if((sockfd_1 = socket(AF_INET, SOCK_STREAM, 0)) < 0){
	//         return -1;
	//     } 
	// 	memset(&serv_addr, '0', sizeof(serv_addr)); 
	// 	serv_addr.sin_family = AF_INET;
	//     serv_addr.sin_port = htons(port); 
	// 	if(inet_pton(AF_INET, ip, &serv_addr.sin_addr)<=0){
	//         return -1;
	//     }

	// 	if(connect(sockfd_1, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
	//        return -1;
	//     } 
	// }
    return s_fd_arr;
}

int sendData(int sockfd, char * buffer, int size){
	int sent = 0, bytes = 0;
    while(sent < size){
		bytes = write(sockfd,buffer + sent, size - sent);
        if (bytes <= 0)
            break;
        sent += bytes;
	}
	if(bytes <= 0) 
		return bytes;
	return sent;
}




int get_actual_size(int serv_index, int size){
	int p_server = n_servers - 1;
	int p_bytes = 0;
	int t_size = size;
	while(t_size > 0){
		int bytes = MIN(BLOCK_SIZE, t_size);
		if(p_server == serv_index)
			p_bytes += bytes;
		

		p_server --;
		if(p_server < 0)
			p_server = n_servers - 1;
		t_size -= bytes;
	}
	printf("server : %d, data size: %d, parity bytes count: %d\n", serv_index, size, p_bytes);
	return size - p_bytes;
}

static int client_getattr(const char *path, struct stat *stbuf)
{
	pthread_mutex_lock(&lock);
	printf("CLIENT_GETATTR %s\n", path);
	int serv_index, sockfd, total = 0, sent = 0, err, path_len; 
	char sendBuffer[1024];
    int * sockfd_arr;

    // return 0;
	if(strlen(path) == 0){
    	pthread_mutex_unlock(&lock);
		return 0;
	}
	sockfd_arr = getSockfd_arr();
	// for(serv_index = 0; serv_index < n_servers; serv_index ++){
	// 	printf("SERVER: %d ---> FD: %d\n", serv_index, sockfd_arr[serv_index]);
	// }

    path_len = strlen(path);
    total += putInt(sendBuffer, GETATTR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
 

    if(f_s == RAID_1){
		for(serv_index = 0; serv_index < n_servers; serv_index ++){
	    	sockfd = sockfd_arr[serv_index];

	    	sent = sendData(sockfd, sendBuffer, total);
		    if(sent <= 0){
	    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
		    	continue;
	    	}
		    
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
			break;

	    }
	}else if(f_s == RAID_5){
		stbuf->st_size = 0;
		int sizes[n_servers];
		for(int i = 0; i < n_servers; i++)
			sizes[i] = 0;
		// int total_size = 0;
		for(serv_index = 0; serv_index < n_servers; serv_index ++){
	    	sockfd = sockfd_arr[serv_index];
	    	sent = sendData(sockfd, sendBuffer, total);
		    if(sent <= 0){
		    	SERV_ERR = CONNECTION_ERR;
		    	mark_down(serv_index);
		    	printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
		    	sizes[serv_index] = -1;
		    	continue;
	    	}
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
				sizes[serv_index] = get_actual_size(serv_index, (off_t) readInt(sockfd));
				// printf("size so far: %d, size to add: %d\n", (int)(stbuf->st_size), data_size);
				// stbuf->st_size += data_size;
				// printf("updated size: %d\n", (int)(stbuf->st_size));
				stbuf->st_atime = time( NULL );
				stbuf->st_mtime = time( NULL );
				stbuf->st_ctime = time( NULL );
			}else{
				printf("could not stat file. returning errno %d\n", err);
				pthread_mutex_unlock(&lock);
				return err;
			}
	    }
	    int max_index = 0;
	    for(int i = 1; i < n_servers; i++){
	    	if(sizes[max_index] < sizes[i]){
	    		max_index = i;
	    	}
	    }
	    for(int i = 0; i < n_servers; i++){
	    	if(sizes[i] == -1){
	    		if(n_servers == 2){
	    			sizes[i] = sizes[1 - i];
	    		}else{
		    		sizes[i] = sizes[max_index];
		    		sizes[i] = MAX(sizes[i], 0);
		    	}
	    	}
	    	stbuf->st_size += sizes[i];
	    }
	}

	printf("GETATTR SIZE************************* %d\n", (int)(stbuf->st_size));
    
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
	int serv_index, sockfd, path_len, total = 0, err, sent = 0, fNum, i, nameLen; 
	int * sockfd_arr;

	char sendBuffer[1024];
    sockfd_arr = getSockfd_arr();
    
    path_len = strlen(path);
    total += putInt(sendBuffer, READDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    
    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
    	sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}

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
    f->fd_arr = malloc(sizeof(int) * n_servers);
    memset(f->fd_arr, 0, sizeof(int) * n_servers);
    int serv_index, sockfd, flags, path_len, total = 0, sent = 0, err;
	char sendBuffer[1024];
    int * sockfd_arr;

    flags = fi->flags;
	sockfd_arr = getSockfd_arr();
    
	path_len = strlen(path);
    total += putInt(sendBuffer, OPEN, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, flags, total);
    
    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
    	sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
		if(err == 0){
			// f->path = malloc(strlen(path) + 1);
			// strcpy(f->path, path);
			f->flags = flags;
			int temp = readInt(sockfd);
			f->fd_arr[serv_index] = temp;
			printf("gadmocemuli fd: %d\n", temp);
			// f->fd_arr[serv_index] = readInt(sockfd);
			printf("OPEN_END: RETURNED FILE DESCRIPTOR - %d\n", f->fd_arr[serv_index]);
		}else{
			printf("OPEN_END: COULD NOT OPEN FILE. SERVER RESPONDED: %d\n", err);
		}
    }
    fi->fh = (long)f;
	pthread_mutex_unlock(&lock);
	return err;
}



int myxor(char * c1, const char * c2, int len){
	int i;
	for(i = 0; i < len; i++){
		c1[i] = (char)(c1[i] ^ c2[i]);
	}
	return 0;
}






int write_block(const char * buffer, int serv_index, int sockfd, int stripe, int path_len, const char * path, int fd, int num_bytes, int b_offset, int r_offset){
	char sendBuffer[1024];
	int total = 0, n_bytes; 
	total += putInt(sendBuffer, WRITE, total); 
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, fd, total);
    total += putInt(sendBuffer, num_bytes, total);
    total += putInt(sendBuffer, stripe * BLOCK_SIZE + b_offset, total);

    if(sendData(sockfd, sendBuffer, total) <= 0){
		SERV_ERR = CONNECTION_ERR;
		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
    	return -1;
	}

	total = 0;
	total += putInt(sendBuffer, num_bytes, total);
    memcpy(sendBuffer + total, buffer + r_offset, num_bytes);
    total += num_bytes;

	if(sendData(sockfd, sendBuffer, total) <= 0){
		SERV_ERR = CONNECTION_ERR;
		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
    	return -1;
	}

    my_errno = readInt(sockfd);
	if(my_errno != 0){
		SERV_ERR = FUNCTION_ERR;
		printf("ALERT! SERVER COULD NOT EXECUTE WRITE SYSCALL ON SERVER, ERRNO:  %d\n", my_errno);
    	return -1;	
	}
	n_bytes = readInt(sockfd);
	return n_bytes;
}



int read_block(char * block, int serv_index, int sockfd, int stripe, int path_len, const char * path, int fd, int num_bytes, int b_offset){
	char sendBuffer[1024];
	int total = 0, n_bytes;
	total += putInt(sendBuffer, READ, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, fd, total);
    total += putInt(sendBuffer, num_bytes, total);
    total += putInt(sendBuffer, stripe * BLOCK_SIZE + b_offset, total);

    if(sendData(sockfd, sendBuffer, total) <= 0){
		SERV_ERR = CONNECTION_ERR;
		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
		return -1;
	}
	my_errno = readInt(sockfd);
	if(my_errno != 0){
		SERV_ERR = FUNCTION_ERR;
		printf("ALERT! SERVER COULD NOT EXECUTE READ, ERRNO: %d\n", my_errno);
    	return -1;	
	}
	n_bytes = readInt(sockfd);
	// if(n_bytes == 0){
	// 	return 0;
	// }
	printf("****bytes read %d ******\n", n_bytes);
	read(sockfd, block, n_bytes);
	return n_bytes;
}

int recover_block(char * block, int serv_index, int stripe, int path_len, const char * path, int fd, int num_bytes, int b_offset){
	printf("STARTING RECOVERING BLOCK:\n");
	int * sockfd_arr = getSockfd_arr();
	int n_bytes = -1;
	char tmp[BLOCK_SIZE]; 
	memset(block, 0, BLOCK_SIZE);
	int ret = 0;
	for(int i = 0; i < n_servers; i++){
		if(i != serv_index){
			memset(tmp, 0, BLOCK_SIZE);
			printf("stripe: %d, serv_index: %d, b_offset: %d, bytes_to_read: %d\n", (int)stripe, (int)i, (int)b_offset, num_bytes);
    		n_bytes = read_block(tmp, i, sockfd_arr[i], stripe, path_len, path, -1, num_bytes, b_offset);
			if(n_bytes == -1){
				mark_down(i);
				return -1;	
			}
			printf("n_bytes %d, content: %s\n", n_bytes, tmp);
			myxor(block, tmp, n_bytes);
			ret = MAX(ret, n_bytes);
		}
	}
	return ret;
}

static int client_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	pthread_mutex_lock(&lock);
	printf("CLIENT_READ %s\n", path);
	struct c_file *f = (struct c_file *)fi->fh;
	int serv_index, sockfd, fd, total = 0, err, sent = 0, n_bytes = 0, path_len = strlen(path);
	char sendBuffer[1024];
    int * sockfd_arr = getSockfd_arr();


    if(f_s == RAID_1){
    	for(serv_index = 0; serv_index < n_servers; serv_index ++){
	    	sockfd = sockfd_arr[serv_index];
		    total = 0;
		    if(f != NULL)
				fd = f->fd_arr[serv_index];
			else
				fd = -1;
		
			total += putInt(sendBuffer, READ, total);
		    total += putInt(sendBuffer, path_len, total);
		    strcpy(sendBuffer + total, path);
		    total += path_len;
		    total += putInt(sendBuffer, fd, total);
		    total += putInt(sendBuffer, size, total);
		    total += putInt(sendBuffer, offset, total);
	    

	    	sent = sendData(sockfd, sendBuffer, total);
		    if(sent <= 0){
	    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
		    	continue;
	    	}
		    err = readInt(sockfd);
			if(err != 0){
		    	continue;	
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
		    break;
	    }
    }else if(f_s == RAID_5){
    	// printf("requested size: %d\n", (int)size);
    	// struct c_file * f = (struct c_file*)(fi->fh);
    	// int r_size = 0;
    	// if(f != NULL){
    	// 	r_size = MIN(size, f->)
    	// }

    	int r_size = size;
    	int w_offset = 0;

    	while(r_size > 0){
    		int b_index = (int)(offset / BLOCK_SIZE);
    		int stripe = b_index / (n_servers - 1);
    		int serv_index = b_index % n_servers;
    		int b_offset = offset % BLOCK_SIZE;
    		char block[BLOCK_SIZE];
    		int bytes_to_read = MIN(BLOCK_SIZE - b_offset, r_size);
	
		    sockfd = sockfd_arr[serv_index];
		    if(f != NULL)
				fd = f->fd_arr[serv_index];
			else
				fd = -1;

			printf("r_size: %d, size: %d, stripe: %d, serv_index: %d, b_offset: %d, w_offset: %d, bytes_to_read: %d\n", (int)r_size, (int)size, (int)stripe, (int)serv_index, (int)b_offset, (int)w_offset, bytes_to_read);
    		
    		n_bytes = read_block(block, serv_index, sockfd, stripe, path_len, path, fd, bytes_to_read, b_offset);
    		if(n_bytes == -1){
    			if(mark_down(serv_index) < 0){
    				pthread_mutex_unlock(&lock);
    				return my_errno;
    			}
				n_bytes = recover_block(block, serv_index, stripe, path_len, path, fd, bytes_to_read, b_offset);
				if(n_bytes == -1){
					pthread_mutex_unlock(&lock);
					return my_errno;
				}
			}
    		
    		if(n_bytes == 0){
    			printf("aq modis vafshe?\n");
    			err = 0;
    			break;
    		}else{
    			memcpy(buf + w_offset, block, n_bytes);
				w_offset += n_bytes;
			    printf("READ_END: BYTES READ: %d\n", n_bytes);

			    offset += n_bytes;
			    r_size -= n_bytes;
			}
		}
		n_bytes = w_offset;
	}

    
    pthread_mutex_unlock(&lock);
	if(err != 0)
		return err;
    return n_bytes;
}





//As for read above, except that it can't return 0.
static int client_write(const char* path, const char *buff, size_t size, off_t offset, struct fuse_file_info* fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_WRITE %s\n", path);
	
	struct c_file *f = (struct c_file *)fi->fh;
	int serv_index, sockfd, fd, path_len = strlen(path), buf_len = strlen(buff), sent = 0, err, send_size, total = 0, b_written, total_written = 0, s_size, buf_offset;
    int * sockfd_arr = getSockfd_arr();

    
    if(f_s == RAID_1){
	    for(serv_index = 0; serv_index < n_servers; serv_index ++){
	    	sockfd = sockfd_arr[serv_index];
	    	buf_offset = 0;
	    	s_size = size;
	    	total = 0;
	    
		    if(f != NULL)
				fd = f->fd_arr[serv_index];
			else
				fd = -1;

			printf("WRITE: FILE DESCRIPTOR - %d\n", fd);
		    
			
			char sendBuffer[4096];
			printf("Size of buffer: %d, Total number of bytes to send: %d\n", (int)buf_len, (int)size);
			total += putInt(sendBuffer, WRITE, total);
		    total += putInt(sendBuffer, path_len, total);
		    strcpy(sendBuffer + total, path);
		    total += path_len;
		    total += putInt(sendBuffer, fd, total);
			
			total += putInt(sendBuffer, size, total);
			total += putInt(sendBuffer, offset, total);


			sent = sendData(sockfd, sendBuffer, total);
			if(sent <= 0){
				printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
		    	continue;
	    	}
			
			while(s_size > 0){
				total = 0;
			    send_size = s_size;
				if(send_size > CHUNK_SIZE)
					send_size = CHUNK_SIZE;
				printf("Trying to send %d bytes over socket. bytes sent so far: %d \n", send_size, buf_offset);
				total += putInt(sendBuffer, send_size, total);
			    memcpy(sendBuffer + total, buff + buf_offset, send_size);
			    total += send_size;

				sent = sendData(sockfd, sendBuffer, total);
				printf("SENT: %d\n", sent);
				if(sent <= 0){
		    		break;
		    	}

			    err = readInt(sockfd);
			    printf("ERR1: %d\n", err);
				if(err != 0){
					printf("aq xoar shemodis?\n");
					break;
		   			// pthread_mutex_unlock(&lock);
					// return err;	
				}
				
				
				b_written = readInt(sockfd);
				printf("B_WRITTEN: %d\n", b_written);
		    	total_written += b_written;
		    	if(b_written < send_size){
					break;    		
		    	}
				s_size -= send_size;
		    	buf_offset += send_size;
		    }
		    if(err != 0){
		    	printf("writing to sockfd %d FAILED. \n", sockfd);
		    }

	    }
	}else if(f_s == RAID_5){
		// int w_size = MAX(0, size - 1);
		int w_size = size;
    	int r_offset = 0;
    	
    	while(w_size > 0){
    		int b_index = (int)(offset / BLOCK_SIZE);
    		int stripe = b_index / (n_servers - 1);
    		int serv_index = b_index % n_servers;
    		int b_offset = offset % BLOCK_SIZE;
    		char old_block[BLOCK_SIZE];
    		int n_bytes = -1;
    		int bytes_to_write = MIN(BLOCK_SIZE - b_offset, w_size);
    		
			printf("w_size: %d, size: %d, stripe: %d, serv_index: %d, b_offset: %d, r_offset: %d, bytes_to_write: %d\n", (int)w_size, (int)size, (int)stripe, (int)serv_index, (int)b_offset, (int)r_offset, bytes_to_write);
    		

		    sockfd = sockfd_arr[serv_index];
		    
		    if(f != NULL)
				fd = f->fd_arr[serv_index];
			else
				fd = -1;

			int old_read = read_block(old_block, serv_index, sockfd, stripe, path_len, path, -1, bytes_to_write, b_offset);
			printf("old_read: %d\n", old_read);
			if(old_read == -1){
				if(mark_down(serv_index) < 0){
					pthread_mutex_unlock(&lock);
					return my_errno;
				}
				old_read = recover_block(old_block, serv_index, stripe, path_len, path, fd, bytes_to_write, b_offset);
				if(old_read == -1){
					pthread_mutex_unlock(&lock);
					return my_errno;
				}
			}
			if(d_servers[serv_index] != -1){
				n_bytes = write_block(buff, serv_index, sockfd, stripe, path_len, path, fd, bytes_to_write, b_offset, r_offset);
			}else{
				n_bytes = bytes_to_write;
			}

			if(n_bytes == 0){
    			printf("aq modis vafshe?\n");
    			break;
    		}else{
				char block_p[BLOCK_SIZE];
				memset(block_p, 0, BLOCK_SIZE);
				int serv_index_p = n_servers - 1 - (stripe % n_servers);
				int sockfd_p = sockfd_arr[serv_index_p];
				// int fd_p;
				// if(f != NULL)
				// 	fd_p = f->fd_arr[serv_index_p];
				// else
				// 	fd_p = -1;
				int p_read = read_block(block_p, serv_index_p, sockfd_p, stripe, path_len, path, -1, n_bytes, b_offset);
				if(p_read == -1){
					mark_down(serv_index_p);
				}else{
					myxor(block_p, old_block, old_read);
					myxor(block_p, buff + r_offset, n_bytes);
					printf("PARITY ABOUT TO BE WRITTEN: %s, n_bytes: %d, stripe: %d, b_offset: %d\n", block_p, n_bytes, stripe, b_offset);
					int parity_write = write_block(block_p, serv_index_p, sockfd_p, stripe, path_len, path, -1, n_bytes, b_offset, b_offset);
					printf("**NUMBER OF BYTES WRITTEN ON PARITY: %d\n", parity_write);
				}
			}

			total_written += n_bytes;
			offset += n_bytes;
			r_offset += n_bytes;
		    w_size -= n_bytes;
		}

	}
    
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
	int serv_index, sockfd, err = -1, total = 0, sent = 0;
	char sendBuffer[1024];
	int * sockfd_arr = getSockfd_arr();
	for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	total = 0;
    	sockfd = sockfd_arr[serv_index];
	    if(f == NULL){
	  // 		pthread_mutex_unlock(&lock);
			// return -EACCES;
			continue;
		}
		if(f->fd_arr[serv_index] <= 0){
			// pthread_mutex_unlock(&lock);
			// return -EACCES;
			continue;
		}
		
		total += putInt(sendBuffer, RELEASE, total);
	    total += putInt(sendBuffer, f->fd_arr[serv_index], total);
	    (void) path;
    
        sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
		printf("release returned value: %d\n", err);
	}
    pthread_mutex_unlock(&lock);
    return err;
}

//Rename the file, directory, or other object "from" to the target "to". 
//Note that the source and target don't have to be in the same directory, so it may be necessary to move the source to an entirely new directory. 
//See rename(2) for full details.
static int client_rename(const char* from, const char* to){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RENAME %s  %s\n", from, to);
	int serv_index, sockfd, total = 0, from_len = strlen(from), to_len = strlen(to), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];
    total += putInt(sendBuffer, RENAME, total);
    total += putInt(sendBuffer, from_len, total);
    strcpy(sendBuffer + total, from);
    total += from_len;
    total += putInt(sendBuffer, to_len, total);
    strcpy(sendBuffer + total, to);
    total += to_len;

    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
		sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

//Remove (delete) the given file, symbolic link, hard link, or special node. 
//Note that if you support hard links, unlink only deletes the data when the last hard link is removed. 
//See unlink(2) for details.
static int client_unlink(const char* path){
	pthread_mutex_lock(&lock);
	printf("CLIENT_UNLINK %s\n", path);
	int serv_index, sockfd, total = 0, path_len = strlen(path), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];
    total += putInt(sendBuffer, UNLINK, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;

    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
		sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

//Remove the given directory. This should succeed only if the directory is empty (except for "." and ".."). See rmdir(2) for details.
static int client_rmdir(const char* path){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RMDIR %s\n", path);
	int serv_index, sockfd, total = 0, path_len = strlen(path), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];
    total += putInt(sendBuffer, RMDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    
    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
		sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

//Create a directory with the given name. The directory permissions are encoded in mode. 
static int client_mkdir(const char* path, mode_t mode){
	pthread_mutex_lock(&lock);
	printf("CLIENT_MKDIR %s\n", path);
	int serv_index, sockfd, total = 0, path_len = strlen(path), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];
    total += putInt(sendBuffer, MKDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    total += putInt(sendBuffer, (int)mode, total);
    
    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

static int client_symlink(const char* from, const char * to){
	pthread_mutex_lock(&lock);
	printf("CLIENT_SYMLINK %s %s\n", from, to);
	int serv_index, sockfd, total = 0, path_len = strlen(from), path_len1 = strlen(to), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];
    total += putInt(sendBuffer, SYMLINK, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, from);
    total += path_len;
    total += putInt(sendBuffer, path_len1, total);
    strcpy(sendBuffer + total, to);
    total += path_len1;

    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

/*  Open a directory for reading. */
static int client_opendir(const char* path, struct fuse_file_info* fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_OPENDIR %s\n", path);
	struct c_file *f = malloc(sizeof(struct c_file));
    memset(f, 0, sizeof(struct c_file));
	f->fd_arr = malloc(sizeof(int) * n_servers);
    memset(f->fd_arr, 0, sizeof(int) * n_servers);

	int serv_index, sockfd, err = -1, total = 0, path_len = strlen(path), sent;
	char sendBuffer[1024];
    
	int * sockfd_arr = getSockfd_arr();
	
	total += putInt(sendBuffer, OPENDIR, total);
    total += putInt(sendBuffer, path_len, total);
    strcpy(sendBuffer + total, path);
    total += path_len;
    
    for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}
		
	    err = readInt(sockfd);
	    if(err == 0){
			f->path = malloc(strlen(path) + 1);
			strcpy(f->path, path);
			f->fd_arr[serv_index] = readInt(sockfd);
		}
	}
	fi->fh = (long)f;
	pthread_mutex_unlock(&lock);
	return err;
}

//This is like release, except for directories.
static int client_releasedir(const char* path, struct fuse_file_info *fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_RELEASEDIR %s\n", path);
	struct c_file *f = (struct c_file *)fi->fh;
	int serv_index, sockfd, total, path_len = strlen(path), err, fd, sent;
	int * sockfd_arr = getSockfd_arr();
	char sendBuffer[1024];

	for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
	    if(f == NULL){
	  //   	pthread_mutex_unlock(&lock);
			// return -EACCES;
			continue;
		}
		if(f->fd_arr[serv_index] <= 0){
			// pthread_mutex_unlock(&lock);
			// return -EACCES;
			continue;
		}
		total = 0;
		fd = f->fd_arr[serv_index];

		total += putInt(sendBuffer, RELEASEDIR, total);
	    total += putInt(sendBuffer, path_len, total);
	    strcpy(sendBuffer + total, path);
	    total += path_len;
	    total += putInt(sendBuffer, fd, total);
	    
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}

		err = readInt(sockfd);
	}
	pthread_mutex_unlock(&lock);
    return err;
}

static int client_utimens(const char*path, const struct timespec ts[2]){
    return 0;
}


static int client_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
	pthread_mutex_lock(&lock);
	printf("CLIENT_CREATE %s\n", path);
	int serv_index, sockfd, total, path_len = strlen(path), err = -1, sent;
    int * sockfd_arr = getSockfd_arr();
	
	char sendBuffer[1024];

	for(serv_index = 0; serv_index < n_servers; serv_index ++){
    	sockfd = sockfd_arr[serv_index];
	    total = 0;
	    total += putInt(sendBuffer, CREATE, total);
	    total += putInt(sendBuffer, path_len, total);
	    strcpy(sendBuffer + total, path);
	    total += path_len;
	    total += putInt(sendBuffer, (int)mode, total);
	    total += putInt(sendBuffer, (int)fi->flags, total);
	    
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}

	    err = readInt(sockfd);
	    if(err != 0){
	    	// pthread_mutex_unlock(&lock);
	    	// return err;
	    	printf("ALERT! COULD NOT CREATE FILE %s ON SERVER %d\n", path, serv_index);
	    	continue;
	    }
		err = readInt(sockfd);
	}
    pthread_mutex_unlock(&lock);
	return 0;
}

static int client_truncate(const char *path, off_t size, struct fuse_file_info *fi){
	pthread_mutex_lock(&lock);
	printf("CLIENT_TRUNCATE %s\n", path);
	struct c_file *f = NULL;// = (struct c_file *)fi->fh;
	int serv_index, sockfd, total = 0, path_len = strlen(path), err = -1, tfd = -1, sent;
	int * sockfd_arr = getSockfd_arr();
	char sendBuffer[1024];
    printf("aq modis? 1\n");


	for(serv_index = 0; serv_index < n_servers; serv_index ++){
		total = 0;
    	sockfd = sockfd_arr[serv_index];
	    if(f == NULL){
			tfd = -1;
		}else if(f->fd_arr[serv_index] <= 0){
			tfd = -1;
		}else{
			tfd = f->fd_arr[serv_index];
		}
		printf("aq modis? 2   %d\n", tfd);

	    total += putInt(sendBuffer, TRUNCATE, total);
	    total += putInt(sendBuffer, path_len, total);
	    strcpy(sendBuffer + total, path);
	    total += path_len;
	    total += putInt(sendBuffer, tfd, total);
	    total += putInt(sendBuffer, (int)size, total);
	    
	    sent = sendData(sockfd, sendBuffer, total);
	    if(sent <= 0){
    		printf("ALERT! COULD NOT CONNECT TO SERVER %d\n", serv_index);
	    	continue;
    	}

	    err = readInt(sockfd);
	    if(err != 0){
	    	// pthread_mutex_unlock(&lock);
	    	// return err;
	    	continue;
	    }
	    
	    err = readInt(sockfd);
	}
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

	close(fd);
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
	// sockfd_1 = -1;
	// sockfd_2 = -1;
	s_fd_arr = NULL;
	n_servers = mounts[1]->nServers; 
	n_servers_down = 0;
	if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init failed\n");
        return 1;
    }
    f_s = RAID_5;
    if(f_s == RAID_5)
    	printf("RAID 55555");
    else printf("RAID 111111");
    // return 0;
	// printf("connecting to ip: %s   port: %d \n", mounts[1]->servers[0]->ip, mounts[0]->servers[0]->port);
	// return 0;
	argv[1] = mounts[1]->mountpoint;
	printf("argv1 : %s\n", argv[1]);
	return fuse_main(argc, argv, &client_oper, NULL);
}

