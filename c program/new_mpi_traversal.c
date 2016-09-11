#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <mpi.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include<malloc.h>
//#include <Python.h>


#define MAXWORDLEN 128
																																													
struct task {
	char *path;
	//int parent_rank;
	struct task *next;
		
}*head_task=NULL; 		// pointer to maintain the head of the task/directory queue


typedef struct files_linked{
	char path[128];
	int weight;
	struct files_linked *next;
}files_linked;

typedef struct file_keyword
	{
	char path[128];
	char keylist[20][25];
	struct files_linked *link;
	struct file_keyword *next;
	}key;

struct process_wait{
	int process_rank;
	struct process_wait *next;
}*head_process_wait=NULL;		// pointer to maintain the header of the waiting process queue. IN this queue those process are stored
	

const char* findWhitespace(const char* text)
    {
        while (*text && !isspace(*text))
            text++;
        return text;
    }

    const char* findNonWhitespace(const char* text)
    {
        while (*text && isspace(*text))
            text++;
        return text;
    }

    typedef struct tagWord
    {
        char word[MAXWORDLEN + 1];
        int count;
    } Word;

    typedef struct tagWordList
    {
        Word* words;
        int count;
    } WordList;

    WordList* createWordList(unsigned int count);

    void extendWordList(WordList* wordList, const int count)
    {
		int i;
        Word* newWords = (Word*)malloc(sizeof(Word) * (wordList->count + count));
        if (wordList->words != NULL) {
            memcpy(newWords, wordList->words, sizeof(Word)* wordList->count);
            free(wordList->words);
        }

        for (i = wordList->count; i < wordList->count + count; i++) {
            newWords[i].word[0] = '\0';
            newWords[i].count = 0;
        }
        wordList->words = newWords;
        wordList->count += count;
    }

    void addWord(WordList* wordList, const char* word)
    {
        assert(strlen(word) <= MAXWORDLEN);
        extendWordList(wordList, 1);
        Word* wordNode = &wordList->words[wordList->count - 1];
        strcpy(wordNode->word, word);
        wordNode->count++;  
    }

    Word* findWord(WordList* wordList, const char* word)
    {
	int i;
        for( i = 0; i < wordList->count; i++) {
            if (strcmp(word, wordList->words[i].word) == 0) {
                return &wordList->words[i];
            }
        }
        return NULL;
    }

    void updateWordList(WordList* wordList, const char* word)
    {
        Word* foundWord = findWord(wordList, word);
        if (foundWord == NULL) {
            addWord(wordList, word);
        } else {
            foundWord->count++;
        }
    }

    WordList* createWordList(unsigned int count)
    {	
	unsigned int i;
        WordList* wordList = (WordList*)malloc(sizeof(WordList));
        if (count > 0) {
            wordList->words = (Word*)malloc(sizeof(Word) * count);
            for(i = 0; i < count; i++) {
                wordList->words[i].count = 0;
                wordList->words[i].word[0] = '\0';
            }
        }
        else {
            wordList->words = NULL;
        }
        wordList->count = count;    
        return wordList;
    }

    void printWords(WordList* wordList)
    {
	int i;
        for ( i = 0; i < wordList->count; i++) {
            printf("%s: %d\n", wordList->words[i].word, wordList->words[i].count);
        }
    }

    int compareWord(const void* vword1, const void* vword2)
    {
        Word* word1 = (Word*)vword1;
        Word* word2 = (Word*)vword2;
        return ((word2->count)-(word1->count));
    }

    void sortWordList(WordList* wordList)
    {
        qsort(wordList->words, wordList->count, sizeof(Word), compareWord);
    }

    key* countWords(const char* text,const char* path)
    {
         WordList   *wordList = createWordList(0);
        Word       *foundWord = NULL;
        const char *beg = findNonWhitespace(text);
        const char *end;
        char       word[MAXWORDLEN];
	key *file_key=(key*)malloc(sizeof(key));
	file_key->next=NULL;
	file_key->link=NULL;
        while (beg && *beg) {
            end = findWhitespace(beg);
            if (*end) {
                assert(end - beg <= MAXWORDLEN);
                strncpy(word, beg, end - beg);
                word[end - beg] = '\0';
                updateWordList(wordList, word);
                beg = findNonWhitespace(end);
            }
            else {
                beg = NULL;
            }
        }

        sortWordList(wordList);
        //printWords(wordList);
	int i=0;
	strcpy(file_key->path,path);
    	while(i<20)
			{           
				
				strcpy(file_key->keylist[i],wordList->words[i].word);
				i++;			
			}
printf("%s\n",file_key->path);
/*for(i=0;i<20;i++)
printf("%s\n",file_key->keylist[i]);*/
free (foundWord);
free (wordList);

return file_key;
    }


int compare2structure(key* s1,key* s2)
{
	int i,j,count=0;
	for(i=0;i<20;i++)
	{
		//printf("%s  %s\n",s1->keylist[i],s2->keylist[i]);
		for(j=0;j<20;j++)
		    {
				if(strcmp(s1->keylist[i],s2->keylist[j])==0)
				{++count;}
		    }

	}
	//printf("\n%d \n",count);
	return count;
}


void print_Directory(struct task *);				//which need some directory to process

int get_lenght(struct process_wait *);

int dir_traversal(char *,key **);

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
	key * head_files_keywords= NULL;
	key * start1= NULL;				
	key * start2= NULL;	
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
				int i=0;
				int j=0;
				start1 = head_files_keywords;
				while( start1 != NULL)
				{
					i++;
					start2 = head_files_keywords;
					while(start2 != NULL)
					{
						if(strcmp(start1->path,start2->path) != 0)
							{
								j = compare2structure(start1,start2);
								if(j)
								{
									printf("\n Comparing @ %d %s ==> %s == > %d ",rank,start1->path,start2->path,j);
								}
							}				
							
						start2=start2->next;
					}
					start1 = start1->next;
				}
				
				printf("\n Hello I am rank %d and I have processed %d files .",rank,i);
				printf("\n Hello I am rank %d and I have recieved termination from the master",rank);
				printf("\n Hello I am rank %d and total memory used by me is %dMB.",rank,(i*sizeof(key)/1000000));
				break;
			}
			//printf("\n Hello I am rank %d and I have recieved directory from the master",rank);
			//printf(" this dir%s\n",name_dir);
			total_files_processed += dir_traversal(name_dir,&head_files_keywords);
		}
		MPI_Send(&total_files_processed, sizeof(int), MPI_INT,0,3, MPI_COMM_WORLD);
		MPI_Recv(&total_files_processed_node, sizeof(int), MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD,&status);
		printf("\n Hello I am rank %d and there are %d files overall (info from server!!) .",rank,total_files_processed_node);
		/*start1 = head_files_keywords;
		while(start1 != NULL)
		{
			start2 = head_files_keywords;
			while(start2 != NULL)
			{
				//if(!strcmp(start1->path,start2->path))				
					//printf("\n Comparing %s ==>> %s",start1->path,start2->path);
				printf("\n Comparing @ %d",rank);
				start2=start2->next;
			}
			start1 = start1->next;		
		}*/
		printf("\n Hello I am rank %d and I have done with internal comparing of the files.",rank);
	}
	return 0;
}

//void dir_traversal(char *dir, int depth, int rank, int num_procs)
int dir_traversal(char *dir,key **head_files)
{
	DIR *dp;
	char *name_dir[100];
	char resolved_path[100]; 
	int count_files=0;
	FILE *f;
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
			f = fopen(resolved_path, "r");
			fseek(f, 0, SEEK_END);
			long fsize = ftell(f);
			fseek(f, 0, SEEK_SET);
			char *string = malloc(fsize + 1);
			fread(string, fsize, 1, f);
			fclose(f);
			string[fsize] = '\0';
			if(*head_files == NULL)
			{
				*head_files = countWords(string,resolved_path);
			}
			else{
				key * start = *head_files;
				while(start->next != NULL)
					start = start->next;
				start->next= countWords(string,resolved_path);
			}		
			free (string);
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
