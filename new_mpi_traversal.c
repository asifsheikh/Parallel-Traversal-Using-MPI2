#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <mpi.h>
//#include <Python.h>
#include <stdlib.h>
																																													
struct task {
	char *path;
	//int parent_rank;
	struct task *next;
		
}*head_task=NULL; 		// pointer to maintain the head of the task/directory queue


struct process_wait{
	int process_rank;
	struct process_wait *next;
}*head_process_wait=NULL;		// pointer to maintain the header of the waiting process queue. IN this queue those process are stored
	

void print_Directory(struct task *);				//which need some directory to process

int get_lenght(struct process_wait *);

int dir_traversal(char *);

int main(int argc, char* argv[])
{	
	int num_procs;
	int rank;
	int j;
	char *topdir, pwd[2]=".";
	char *name_dir[100]= {};
	MPI_Status status;
	int total_files_processed = 0;
	int total_files_processed_node = 0;
	struct process_wait *head1 = NULL;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);
	if(rank == 0)
	{
		//printf("my rank is: %d\n",rank);
		if (argc != 2)
			topdir=pwd;
		else
			topdir=argv[1];
		struct task *new_task;
		int count_from_nodes=0;
		int num_of_nodes=0;
		new_task = (struct task*)malloc(sizeof(struct task));
		new_task->path=topdir;
		new_task->next=NULL;
		head_task=new_task;
		while(1)
		{		
			
			//printf("\n %d I am rank 0 and am waiting for request!",get_lenght(head_process_wait));			
			MPI_Recv(name_dir, 100, MPI_UNSIGNED_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
			
			if(status.MPI_TAG == 0)
			{	
				//printf("\n Hello I am  rank %d I have recived directory req from process %d and tag %d",rank,status.MPI_SOURCE,status.MPI_TAG);
				//printf("\n Before Sending");
				//print_Directory(head_task); // printing the contains of the directory check				
				if(head_task != NULL)
				{	
					struct task *temp = head_task;
					head_task=head_task->next;			
					MPI_Send(temp->path, 100, MPI_UNSIGNED_CHAR, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD);
				//printf("\n After Sending");
				//print_Directory(head_task); // printing the contains of the directory check				
				}
				else{
									
					struct process_wait *start=NULL;
					struct process_wait *newprocess = (struct process_wait*)malloc(sizeof(struct process_wait));
					newprocess->process_rank = status.MPI_SOURCE;
					newprocess->next=NULL;
					start = head_process_wait ;
					if(start!=NULL)
					{	
						while(start->next!=NULL)
							start=start->next;
						start->next=newprocess;
					}
					else{
				
						head_process_wait=newprocess;
					}
				}
			}
			else if(status.MPI_TAG == 1)
			{

				struct task *start;
				struct task *new_task = (struct task*)malloc(sizeof(struct task));
				//printf("Process 0 received %s\n from Process %d\n",name_dir,status.MPI_SOURCE);
				//printf("\nBefore REceiving ");
				//print_Directory(head_task);				
				new_task->path=(char*)malloc(100);
				strcpy(new_task->path,name_dir);
				new_task->next=NULL;
				//printf("\n Hello I am  rank %d I have recived found directory req %s from process %d and tag %d",rank,name_dir,status.MPI_SOURCE,status.MPI_TAG);				
				start =  head_task;
				if(start!=NULL)
				{	
					while(start->next!=NULL)
						start=start->next;
					start->next=new_task;
				}
				else
				{
					head_task=new_task;
				}
					
				//printf("\nAfter REceiving from rank %d",status.MPI_SOURCE);
				//print_Directory(head_task);  // check for printing the head contains of the directory.
				//exit(0);
				
				while(head_process_wait !=NULL && head_task != NULL)
				{
					//remove one directory from the task_queue
					// ASSIGN that directory to the waiting process;
					
					struct task* mtask = head_task;
					struct process_wait *mprocess = head_process_wait;
					head_task=head_task->next;					
					head_process_wait = head_process_wait->next;
					MPI_Send(mtask->path, 100, MPI_UNSIGNED_CHAR, mprocess->process_rank, 0, MPI_COMM_WORLD);

				}
				
				
				
			}
						
			/*
				1. If the head_task == NULL
				2. AND if the process wait queue is equal to MPI_COMM_WORLD -1
				3. MPI_Send("", 100, MPI_UNSIGNED_CHAR, SOURCE,2, MPI_COMM_WORLD)  // MPI TAG 2 for indicating the termination 						condition
			*/
			
			if((head_task==NULL) && (get_lenght(head_process_wait) == (num_procs - 1) )){
				head1 = head_process_wait;
				printf("\n Hello I guess the processing of the directory is over.. this is server");				
				while(head1!=NULL)
				{
					
					MPI_Send(name_dir, 100, MPI_UNSIGNED_CHAR, head1->process_rank,2, MPI_COMM_WORLD);  // MPI TAG 2 for indicating the termination condition
									
					head1 = head1->next;
				}
				break;	
				
			}		
		}
		while(1)
		{		
			MPI_Recv(&count_from_nodes, sizeof(int), MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
			if(status.MPI_TAG == 3)			// TAG 3 is for identifying that the sender is sending file counts
			{	
				total_files_processed += count_from_nodes;
				num_of_nodes++;
			}
			if(num_of_nodes == (num_procs-1))
			{
				printf("\n The Total number of files processed overall is %d",total_files_processed);
				/*
					Sending the Total number of files to each and every node so that they can prepare the global data structure

				*/
				for(j=1;j<num_procs;j++)
					MPI_Send(&total_files_processed, sizeof(int), MPI_INT,j,4,MPI_COMM_WORLD);
				break;
			}
		}				
		
	}
	else{
		while(1)
		{
			//printf("\nI am rank %d i am idel (send some directory) ",rank);			
			MPI_Send(name_dir, 100, MPI_UNSIGNED_CHAR, 0, 0, MPI_COMM_WORLD);
			//printf("\n Hello I am not rank 0 and i guess I have sent something to the rank 0 ie master");
			MPI_Recv(name_dir, 100, MPI_UNSIGNED_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
			if(status.MPI_TAG == 2)
			{
				printf("\n Hello I am rank %d and I have processed %d files .",rank,total_files_processed);				
				printf("\n Hello I am rank %d and I have recieved termination from the master",rank);
				break;
			}
			//printf("\n Hello I am rank %d and I have recieved directory from the master",rank);
			//printf(" this dir%s\n",name_dir);
			total_files_processed += dir_traversal(name_dir);
		}
		MPI_Send(&total_files_processed, sizeof(int), MPI_INT,0,3, MPI_COMM_WORLD);
		MPI_Recv(&total_files_processed_node, sizeof(int), MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
		printf("\n Hello I am rank %d and I have processed %d files .",rank,total_files_processed_node);	
	}
	return 0;
}

//void dir_traversal(char *dir, int depth, int rank, int num_procs)
int dir_traversal(char *dir)
{
	DIR *dp;
	char *name_dir[100];
	char resolved_path[100]; 
	int count_files=0;
	//char *name_dir;
	struct dirent *entry;
	struct stat statbuf;
	if((dp = opendir(dir)) == NULL) {
        	fprintf(stderr,"cannot open directory: %s\n", dir);
	        return;
	}
	
	//chdir(dir);
	while((entry = readdir(dp)) != NULL){
		lstat(entry->d_name,&statbuf);
		//if(S_ISDIR(statbuf.st_mode))
		if(entry->d_type == DT_DIR)
		{
								
					if(strcmp(".",entry->d_name) == 0 || strcmp("..",entry->d_name) == 0)
		        			continue;
					strcpy(name_dir,dir);
					strcat(name_dir,"/");
					strcat(name_dir,entry->d_name);
					//printf("\n Sending %s\n",name_dir);
					MPI_Send(name_dir, 100, MPI_UNSIGNED_CHAR, 0, 1, MPI_COMM_WORLD);	
		}
		else{
			if(strcmp(".",entry->d_name) == 0 || strcmp("..",entry->d_name) == 0)
                		continue;
			count_files++;
			
        		strcpy(resolved_path,dir);
			strcat(resolved_path,"/");
			strcat(resolved_path,entry->d_name);
        		//printf("\n%s\n",resolved_path); 
			//printf("%s\n",entry->d_name);
			/*Py_Initialize();
			PyRun_SimpleString("############@@@@@@@@@");
			Py_Finalize();*/	
					
			
		}
	}	
	//chdir("..");
    	closedir(dp);
	return count_files;
}


void print_Directory(struct task *head)
{
	struct task *start;
	int i =1;
	if(head == NULL)
		printf("\n HEAD_TASK =  NULL");
	else{
		start = head;
		printf("\n PRINTING DIRECTORY ");
		while(start != NULL){
			printf("\n %d \t %s ", i++,start->path);
			start = start->next;		
		}
	}

}

int get_lenght(struct process_wait *head)
{
	int count =0 ;
	if(head == NULL)
		return 0;
	else{
		while(head != NULL)
		{
			count ++;
			head = head->next;
		}
	}
	return count;
}
