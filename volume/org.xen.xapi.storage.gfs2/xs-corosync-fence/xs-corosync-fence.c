#include <stdio.h>
#include <stdlib.h>
#include <linux/watchdog.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/resource.h>
#define WATCHDOG_DEVICE "/dev/watchdog"
#define PROCESS_PRIORITY -20

/*TO-DO: place logs in appropriate files
void print_log(char *message)
{

}*/

void xs_corosync_fence(int argc, char **argv)
{
    int watchdog_device_handle,device_handle,pulse;
    char *buffer;

    /* first open the device else watchdog would restart the
     system when there is an error at this step */

    device_handle = open(argv[1],  O_SYNC|O_RDWR|O_DIRECT);
    if(-1 == device_handle)
    {
       printf("cannot open device \n" );
       return;
    }

    /* set the seek pointer to the block specified by arg 3 (2*node_id) */

    if(0 > lseek(device_handle, 2*atoi(argv[2]), SEEK_SET))
    {
        printf("error moving the seek pointer \n");
        return;
    }

    watchdog_device_handle = open(WATCHDOG_DEVICE, O_RDWR);
    if(-1 == watchdog_device_handle)
    {
       printf("cannot open watchdog device \n");
       close(device_handle);
       return;
    }

    if (0 != ioctl(watchdog_device_handle, WDIOC_SETTIMEOUT, argv[3]))
    {
       printf("error in setting interval, using default value of 60s \n");
    }
    while(1)
    {
       if(1 == read(device_handle, buffer, 1))
        {
            if(0 == atoi(buffer))
            {
               if(0 != ioctl(watchdog_device_handle, WDIOC_KEEPALIVE, NULL))
               {
                   printf("write to device failed \n");
                   break;
               }
               sleep(atoi(argv[4]));
               printf("tick: %d \n", pulse++);
            }
            else if (1 == atoi(buffer))
            {
                /* kill host */
                lseek(device_handle, 2*atoi(argv[2]) +1, SEEK_SET);
                /* write to (2N + 1)th block in device */
                if( -1 == write(device_handle, "1", 1))
                {
                    printf("write to device failed \n");
                    break;
                }
                break;
            }

            else if (2 == atoi(buffer))
            {
                /* exit gracefully */
                if(-1 == write(device_handle, "V", 1))
                    printf("write to device failed \n");
                break;

            }
        }
       else
       {
           printf("failed to read from device \n");
            break;
       }

    }

    close(watchdog_device_handle);
    close(device_handle);
}

int main(int argc, char **argv)
{
    setpriority(PRIO_PROCESS, 0, PROCESS_PRIORITY);
    if(5 != argc)
    {
      printf("usage: xs-corosync-fence <block device path> <node ID> <WD timeout> <main loop frequency> \n");
      return 1;
    }

    xs_corosync_fence(argc, argv);
    return 1;

}

