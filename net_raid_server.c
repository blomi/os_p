#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h> 

#include <dirent.h>
#include <string.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>


char * server_ip;
int server_port;
char * storage_dir;

enum func{READDIR, GETATTR, OPEN, READ, MKDIR, RMDIR, UNLINK, SYMLINK, RELEASE, WRITE, OPENDIR, RELEASEDIR, RENAME, CREATE, TRUNCATE};
enum file_type{ISREG, ISDIR, ISCHR, ISBLK, ISFIFO, ISLNK, ISSOCK, NOFILE};  //N_FILE = NO FILE EXISTED, DIR = DIRECTORY, R_FILE = REGULAR FILE


int sendData(int connfd, char * data, int data_len){
	int total = data_len;
    printf("total???  %d\n", total);
    int sent = 0, bytes = 0;
    do {
        bytes = write(connfd,data+sent,total-sent);
        if (bytes < 0) // tu bytes == 0, mashin ra unda vqna zustad ar vici
            break;
        sent+=bytes;
    } while (sent < total);
    return sent;
}

int readInt(int connfd){
    char tmp[8];
    recv(connfd, tmp, 8, 0);
    uint32_t raw = *(uint32_t*)tmp;
    return ntohl(raw);
}

int putInt(char * buffer, int num, int offset){
    uint32_t tmp;
    tmp = htonl(num);
    memcpy(buffer + offset, (char*)&tmp, 8);
    return 8;
}

int s_readdir(int connfd, char * path){
	DIR * dir;
	struct dirent * ent;
	char sendBuffer[1024];
    int total = 0, fNum = 0, sent = 0, tmpInd;
    if((dir = opendir(path)) == NULL)
		total += putInt(sendBuffer, -errno, total);
	else{
		total += putInt(sendBuffer, 0, total);
		tmpInd = total;
		total += 8;
		while ((ent = readdir(dir)) != NULL){
			fNum = fNum + 1;
			total += putInt(sendBuffer, (strlen(ent->d_name)), total);
			strcpy(sendBuffer + total, ent->d_name);
			total += strlen(ent->d_name);
			total += putInt(sendBuffer, ent->d_ino, total);
			total += putInt(sendBuffer, ent->d_type<<12, total);
			
		}
		putInt(sendBuffer, fNum, tmpInd);
		closedir(dir);
	}
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_opendir(int connfd, char * path){
	char sendBuffer[1024];
    int total = 0, sent;
	DIR * dirp = opendir(path);
	if(dirp == NULL){
		printf("could not open directory: %s\n", path);
		total += putInt(sendBuffer, -errno, total);
	}else{
		printf("directory  %s opened\n, file descriptor: %d\n", path, (int)dirp);
		total += putInt(sendBuffer, 0, total);
		total += putInt(sendBuffer, (size_t)dirp, total);
	}
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_releasedir(int connfd, char* path, int fd){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = closedir((DIR*)((size_t)fd));
	if (err == -1)
		total += putInt(sendBuffer, -errno, total);
	else
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}


int s_open(int connfd, char * path, int flags){
	char sendBuffer[1024];
    int total = 0, fd, sent = 0;
	fd = open(path, flags);
	if (fd < 0){
		printf("could not open file: %s\n", path);
		total += putInt(sendBuffer, -errno, total);
	}else{
		printf("file - %s -  opened succesfully, file descriptor: %d\n", path, fd);
		total += putInt(sendBuffer, 0, total);
		total += putInt(sendBuffer, fd, total);
	}
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}



int num_dirs(const char* path)
{
    int dir_count = 0;
    struct dirent* dent;
    DIR* srcdir = opendir(path);

    if (srcdir == NULL)
    {
        printf("No DIRECTORY");
        return -1;
    }

    while((dent = readdir(srcdir)) != NULL)
    {
        struct stat st;

        if(strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
            continue;

        if (fstatat(dirfd(srcdir), dent->d_name, &st, 0) < 0)
        {
            perror(dent->d_name);
            continue;
        }

        if (S_ISDIR(st.st_mode)) dir_count++;
    }
    closedir(srcdir);
    return dir_count;
}

int s_getattr(int connfd, char * path){
	char sendBuffer[1024];
	int total = 0, sent, error;
	struct stat path_stat;
	if ((error = stat(path, &path_stat)) < 0){
		total += putInt(sendBuffer, -errno, total);
		printf("could not stat file\n");
	}else{
		total += putInt(sendBuffer, 0, total);
		//total += putInt(sendBuffer, path_stat.st_dev, total);
		//total += putInt(sendBuffer, path_stat.st_ino, total);
		total += putInt(sendBuffer, (int)path_stat.st_mode, total);
		total += putInt(sendBuffer, (int)path_stat.st_nlink, total);
		total += putInt(sendBuffer, (int)path_stat.st_uid, total);
		total += putInt(sendBuffer, (int)path_stat.st_gid, total);
		total += putInt(sendBuffer, (int)path_stat.st_rdev, total);
		//total += putInt(sendBuffer, path_stat.st_blksize, 0);
		total += putInt(sendBuffer, (int)path_stat.st_blocks, total);
		total += putInt(sendBuffer, (int)path_stat.st_size, total);
	}

    sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_mkdir(int connfd, char * path, int mode){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = mkdir(path, (mode_t)mode);
	if(err == -1)
		total += putInt(sendBuffer, -errno, total);
	else 
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_rmdir(int connfd, char * path){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = rmdir(path);
	if(err == -1)
		total += putInt(sendBuffer, -errno, total);
	else 
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_unlink(int connfd, char * path){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = unlink(path);
	if(err == -1)
		total += putInt(sendBuffer, -errno, total);
	else 
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_symlink(int connfd, char * from, char * to){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = symlink(from, to);
	if(err == -1)
		total += putInt(sendBuffer, -errno, total);
	else 
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_rename(int connfd, char * from, char * to){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = rename(from, to);
	if(err == -1)
		total += putInt(sendBuffer, -errno, total);
	else 
		total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_release(int connfd, int fd){
	char sendBuffer[1024];
	int total = 0, sent;
	close(fd);
	total += putInt(sendBuffer, 0, total);
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_read(int connfd, char * path, int fd, int size, int offset){
	int fsize = lseek(fd, 0, SEEK_END);
	char buff[1024];     //es zoma sheidzleba ar iyos sakmarisi didi failebistvis da shesacvleli maq. droebit iyos ase
    char sendBuffer[1024];
    int total = 0, b_read, sent = 0, coread = 0;//cor = close_on_read
    if(fd == -1){
    	fd = open(path, O_RDONLY);
    	if(fd == -1)
    		total += putInt(sendBuffer, -errno, total);
    	else
    		coread = 1;
    }
    if(fd != -1){
    	b_read = pread(fd, buff, size, offset);
		if(b_read == -1)
			total += putInt(sendBuffer, -errno, total);
		else{
			total += putInt(sendBuffer, 0, total);
			total += putInt(sendBuffer, b_read, total);
			memcpy(sendBuffer + total, buff, b_read);
			total += b_read;
		}
	}
	if(coread == 1)
		close(fd);
    sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_write(int connfd, char * path, int fd, char * buff, int size, int offset){
	char sendBuffer[1024];
	int total = 0, sent, err, b_written, cowrite = 0;//cow = close_on_write

	if(fd == -1){
    	fd = open(path, O_WRONLY);
    	if(fd == -1)
    		total += putInt(sendBuffer, -errno, total);
    	else
    		cowrite = 1;
    }
    if(fd != -1){
    	b_written = pwrite(fd, buff, size, offset);
		if(b_written == -1)
			total += putInt(sendBuffer, -errno, total);
		else{
			total += putInt(sendBuffer, 0, total);
			total += putInt(sendBuffer, b_written, total);
		}
	}

	if(cowrite == 1)
		close(fd);
	sent = 	sendData(connfd, sendBuffer, total);
	return sent;	
}

int s_create(int connfd, char * path, int mode, int flags){
	char sendBuffer[1024];
	int total = 0, sent, err;
	err = open(path, flags, mode);
	if (err == -1)
		total += putInt(sendBuffer, -errno, total);
	else{
		total += putInt(sendBuffer, 0, total);
		total += putInt(sendBuffer, err, total);
	}
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}

int s_truncate(int connfd, char * path, int fd, int size){
	char sendBuffer[1024];
	int total = 0, sent, err;
	if(fd != -1){
		err = ftruncate(fd, size);
	}else{
		err = truncate(path, size);
	}
	if (err == -1)
		total += putInt(sendBuffer, -errno, total);
	else{
		total += putInt(sendBuffer, 0, total);
		total += putInt(sendBuffer, err, total);
	}
	sent = sendData(connfd, sendBuffer, total);
	return sent;
}



int processData(int connfd){
	int path_len, size, offset, ret, fd, flags, mode, path_len1, buf_len, from_len, to_len;
	char path[128], path1[128], p_from[128], p_to[128];
	char buffer[1024];
	enum func f = readInt(connfd);
	char * fullpath;
	char * fullpath1;
	// return 0;

	
	switch (f){
		case READDIR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			printf("Command:\tREADDIR\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_readdir(connfd, fullpath);
			free(fullpath);
			break;
		case GETATTR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			printf("Command:\tGETATTR\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_getattr(connfd, fullpath);
			free(fullpath);
			break;
		case OPEN:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			flags = readInt(connfd);
			printf("Command:\tOPEN\nPath Length:\t%d\nPath:\t\t%s\nFlags:\t\t%d\n", path_len, fullpath, flags);
			ret = s_open(connfd, fullpath, flags);
			free(fullpath);
			break;
		case READ:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			fd = readInt(connfd);
			size = readInt(connfd);
			offset = readInt(connfd);
			printf("Command:\tREAD\nPath Length:\t%d\nPath:\t\t%s\nFD:\t%d\nSize:\t\t%d\nOffset:\t\t%d\n", path_len, path, fd, size, offset);
			ret = s_read(connfd, fullpath, fd, size, offset);
			free(fullpath);
			break;
		case MKDIR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			mode = readInt(connfd);
			printf("Command:\tMKDIR\nPath Length:\t%d\nPath:\t\t%s\nMODE:\t%d\n", path_len, fullpath, mode);
			ret = s_mkdir(connfd, fullpath, mode);
			free(fullpath);
			break;
		case RMDIR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			printf("Command:\tRMDIR\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_rmdir(connfd, fullpath);
			free(fullpath);
			break;
		case UNLINK:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			printf("Command:\tUNLINK\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_unlink(connfd, fullpath);
			free(fullpath);
			break;
		case SYMLINK:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			path_len1 = readInt(connfd);
			recv(connfd, &path1, path_len1, 0);
			path1[path_len1] = '\0';
			fullpath1 = malloc(strlen(storage_dir) + strlen(path1));
			sprintf(fullpath1, "%s%s", storage_dir, path1 + 1);
			printf("Command:\tSYMLINK\nPath Length:\t%d\nPath:\t\t%s\nPath1 Length:\t%d\nPath1\t%s\n", path_len, fullpath, path_len1, fullpath1);
			ret = s_symlink(connfd, fullpath, fullpath1);
			free(fullpath);
			free(fullpath1);
			break;
		case RELEASE:
			fd = readInt(connfd);
			printf("Command:\tRELEASE\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_release(connfd, fd);
			break;
		case WRITE:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			fd = readInt(connfd);
			buf_len = readInt(connfd);
			recv(connfd, &buffer, buf_len, 0);
			size = readInt(connfd);
			offset = readInt(connfd);
			printf("Command:\tWRITE\nPath Length:\t%d\nPath:\t\t%s\nBuffer Length:\t%d\nBuffer:\t%s\nSize\t\t%d\nOffset\t\t%d\nfd\t\t%d\n", path_len, fullpath, buf_len, buffer, size, offset, fd);
			ret = s_write(connfd, fullpath, fd, buffer, size, offset);
			free(fullpath);
			break;
		case OPENDIR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			printf("Command:\tOPENDIR\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_opendir(connfd, fullpath);
			free(fullpath);
			break;
		case RELEASEDIR:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			fd = readInt(connfd);
			printf("Command:\tRELEASEDIR\nPath Length:\t%d\nPath:\t\t%s\n", path_len, fullpath);
			ret = s_releasedir(connfd, fullpath, fd);
			free(fullpath);
			break;
		case RENAME:
			from_len = readInt(connfd);
			recv(connfd, &p_from, from_len, 0);
			p_from[from_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(p_from));
			sprintf(fullpath, "%s%s", storage_dir, p_from + 1);
			to_len = readInt(connfd);
			recv(connfd, &p_to, to_len, 0);
			p_to[to_len] = '\0';
			fullpath1 = malloc(strlen(storage_dir) + strlen(p_to));
			sprintf(fullpath1, "%s%s", storage_dir, p_to + 1);
			printf("Command:\tRENAME\tFrom Length:\t%d\nFrom:\t\t%s\nTo Length:\t%d\nTo\t%s\n", from_len, fullpath, to_len, fullpath1);
			ret = s_rename(connfd, fullpath, fullpath1);
			free(fullpath);
			free(fullpath1);
			break;
		case CREATE:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			mode = readInt(connfd);
			flags = readInt(connfd);
			printf("Command:\tCREATE\nPath Length:\t%d\nPath:\t\t%s\nmode:\t%d\nflags:\t%d\n", path_len, fullpath, mode, flags);
			ret = s_create(connfd, fullpath, mode, flags);
			free(fullpath);
			break;
		case TRUNCATE:
			path_len = readInt(connfd);
			recv(connfd, &path, path_len, 0);
			path[path_len] = '\0';
			fullpath = malloc(strlen(storage_dir) + strlen(path));
			sprintf(fullpath, "%s%s", storage_dir, path + 1);
			fd = readInt(connfd);
			size = readInt(connfd);
			printf("Command:\tTRUNCATE\nPath Length:\t%d\nPath:\t\t%s\nfd:\t%d\nsize:\t%d\n", path_len, fullpath, fd, size);
			ret = s_truncate(connfd, fullpath, fd, size);
			free(fullpath);
			break;
	}
	

	// printf("function:: %d  %d\n", ntohl(fun), n);
	return ret;
}


int main(int argc, char *argv[])
{

	if(argc != 4)
    {
        printf("program parameters wrongly specified, exitting..\n");
        return 1;
    } 

    server_ip = malloc(strlen(argv[1]) + 1);
    strcpy (server_ip, argv[1]);
    server_port = strtol(argv[2], (char **)NULL, 10);
    storage_dir = malloc(strlen(argv[3]) + 1);
    strcpy (storage_dir, argv[3]);
        
    // printf("server ip: %s\nserver port: %d\nstorage directory: %s\n", server_ip, server_port, storage_dir);
    

    /*iwyeba imati kodi*/

   	int listenfd = 0, connfd = 0;
    struct sockaddr_in serv_addr; 

    // char sendBuff[1025];
    

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, '0', sizeof(serv_addr));
    // memset(sendBuff, '0', sizeof(sendBuff)); 
    char * sendBuff = "momivida sheni data!";

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(server_port); 

    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 

    listen(listenfd, 10); 

    int n = 0, k;
  	// char recvBuff[1024];
  	struct sockaddr_in conn_addr;
  	unsigned int len = sizeof(conn_addr);

    while(1)
    {
    	printf("listening:\n");
    	connfd = accept(listenfd, (struct sockaddr*)NULL, NULL);
		int bytes = processData(connfd);
		// n = recv(connfd, &recvBuff, 1023, 0);
     	// printf("Recd: %d bytes\n",bytes);
     	// printf("%s\n", recvBuff);
     	// for (k=0; k<16; ++k) { printf("%d: 0x%02X, \n", k, recvBuff[k]); }
        // int sent = sendData(connfd, sendBuff);
        // printf("sent: %d,  data length: %d, data: %s \n\n", bytes, (int)strlen(sendBuff), sendBuff); 
		// sleep(1);
    }
	



	return 0;
}