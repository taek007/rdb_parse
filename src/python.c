#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
int main(){
    int status = 100;
    int execres = 100;
    //参数，/usr/bin/python前的“／“不要漏
    char *argvlist[]={"/usr/bin/python", "1.py", NULL};
    printf("start\n");
    if (fork()!=0){
        waitpid(-1,&status,0);
        printf("parent process finished, status is %d\n", status);
    }else{
        printf("child process\n");
        execres = execve(argvlist[0],argvlist,NULL);
        //如果调用execve成功的话，以下打印的东西是不可见的，因为exec会将原来进程中的内容全部替换掉
        printf("child process runing, res is %d\n",execres);
    }
}