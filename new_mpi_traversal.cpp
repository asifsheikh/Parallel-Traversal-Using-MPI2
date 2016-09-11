#include <unistd.h>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <ctype.h>
#include <mpi.h>
//#include "new_file_traversal.cpp"
//#include <Python.h>
#include <stdlib.h>
																																													
//extern  void printKMostFreq( FILE* fp, int k );

# define MAX_CHARS 100
# define MAX_WORD_SIZE 100

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



 
// A Trie node
struct TrieNode
{
    bool isEnd; // indicates end of word
    unsigned frequency;  // the number of occurrences of a word
    int indexMinHeap; // the index of the word in minHeap
    TrieNode* child[MAX_CHARS]; // represents 26 slots each for 'a' to 'z'.
};
 
// A Min Heap node
struct MinHeapNode
{
    TrieNode* root; // indicates the leaf node of TRIE
    unsigned frequency; //  number of occurrences
char* word; // the actual word stored
};
 
// A Min Heap
struct MinHeap
{
    unsigned int capacity; // the total size a min heap
    int count; // indicates the number of slots filled.
    MinHeapNode * array; //  represents the collection of minHeapNodes
};
 
// A utility function to create a new Trie node
TrieNode* newTrieNode()
{
    // Allocate memory for Trie Node
    TrieNode* trieNode = new TrieNode;
 
    // Initialize values for new node
    trieNode->isEnd = 0;
    trieNode->frequency = 0;
    trieNode->indexMinHeap = -1;
    for( int i = 0; i < MAX_CHARS; ++i )
        trieNode->child[i] = NULL;
 
    return trieNode;
}
 
// A utility function to create a Min Heap of given capacity
MinHeap* createMinHeap( int capacity )
{
    MinHeap* minHeap = new MinHeap;
 
    minHeap->capacity = capacity;
    minHeap->count  = 0;   

	// Allocate memory for array of min heap nodes
    minHeap->array = new MinHeapNode [ minHeap->capacity ];
 
    return minHeap;
}
 
// A utility function to swap two min heap nodes. This function
// is needed in minHeapify
void swapMinHeapNodes ( MinHeapNode* a, MinHeapNode* b )
{
    MinHeapNode temp = *a;
    *a = *b;
    *b = temp;
}
 
// This is the standard minHeapify function. It does one thing extra.
// It updates the minHapIndex in Trie when two nodes are swapped in
// in min heap
void minHeapify( MinHeap* minHeap, int idx )
{
    int left, right, smallest;
 
    left = 2 * idx + 1;
    right = 2 * idx + 2;
    smallest = idx;
    if ( left < minHeap->count &&
         minHeap->array[ left ]. frequency <
         minHeap->array[ smallest ]. frequency
       )
        smallest = left;
 
    if ( right < minHeap->count &&
         minHeap->array[ right ]. frequency <
         minHeap->array[ smallest ]. frequency
       )
        smallest = right;
 
    if( smallest != idx )
    {
        // Update the corresponding index in Trie node.
        minHeap->array[ smallest ]. root->indexMinHeap = idx;
        minHeap->array[ idx ]. root->indexMinHeap = smallest;
 
        // Swap nodes in min heap
        swapMinHeapNodes (&minHeap->array[ smallest ], &minHeap->array[ idx ]);
 
        minHeapify( minHeap, smallest );
    }
}
 
// A standard function to build a heap
void buildMinHeap( MinHeap* minHeap )
{
    int n, i;
    n = minHeap->count - 1;
 
    for( i = ( n - 1 ) / 2; i >= 0; --i )
        minHeapify( minHeap, i );
}
 
// Inserts a word to heap, the function handles the 3 cases explained above
void insertInMinHeap( MinHeap* minHeap, TrieNode** root, const char* word )
{
    // Case 1: the word is already present in minHeap
    if( (*root)->indexMinHeap != -1 )
    {
        ++( minHeap->array[ (*root)->indexMinHeap ]. frequency );
 
        // percolate down
        minHeapify( minHeap, (*root)->indexMinHeap );
    }
 
    // Case 2: Word is not present and heap is not full
    else if( minHeap->count < minHeap->capacity )
    {
        int count = minHeap->count;
        minHeap->array[ count ]. frequency = (*root)->frequency;
        minHeap->array[ count ]. word = new char [strlen( word ) + 1];
        strcpy( minHeap->array[ count ]. word, word );
 
        minHeap->array[ count ]. root = *root;
        (*root)->indexMinHeap = minHeap->count;
 
        ++( minHeap->count );
        buildMinHeap( minHeap );
    }
 
    // Case 3: Word is not present and heap is full. And frequency of word
    // is more than root. The root is the least frequent word in heap,
    // replace root with new word
    else if ( (*root)->frequency > minHeap->array[0]. frequency )
    {
 
        minHeap->array[ 0 ]. root->indexMinHeap = -1;
        minHeap->array[ 0 ]. root = *root;
        minHeap->array[ 0 ]. root->indexMinHeap = 0;
        minHeap->array[ 0 ]. frequency = (*root)->frequency;
 
        // delete previously allocated memoory and
        delete [] minHeap->array[ 0 ]. word;
        minHeap->array[ 0 ]. word = new char [strlen( word ) + 1];
        strcpy( minHeap->array[ 0 ]. word, word );
 
        minHeapify ( minHeap, 0 );
    }
}
 
// Inserts a new word to both Trie and Heap
void insertUtil ( TrieNode** root, MinHeap* minHeap,
                        const char* word, const char* dupWord )
{
    // Base Case
    if ( *root == NULL )
        *root = newTrieNode();
 
    //  There are still more characters in word
    if ( *word != '\0' )
        insertUtil ( &((*root)->child[ tolower( *word ) - 97 ]),
                         minHeap, word + 1, dupWord );
    else // The complete word is processed
    {
        // word is already present, increase the frequency
        if ( (*root)->isEnd )
            ++( (*root)->frequency );
        else
        {
            (*root)->isEnd = 1;
            (*root)->frequency = 1;
        }
 
        // Insert in min heap also
        insertInMinHeap( minHeap, root, dupWord );
    }
}
 
 
// add a word to Trie & min heap.  A wrapper over the insertUtil
void insertTrieAndHeap(const char *word, TrieNode** root, MinHeap* minHeap)
{
    insertUtil( root, minHeap, word, word );
}
 
// A utility function to show results, The min heap
// contains k most frequent words so far, at any time
void displayMinHeap( MinHeap* minHeap )
{
    int i;
 
    // print top K word with frequency
    for( i = 0; i < minHeap->count; ++i )
    {
        printf( "%s : %d\n", minHeap->array[i].word,
                            minHeap->array[i].frequency );
    }
}
 
// The main funtion that takes a file as input, add words to heap
// and Trie, finally shows result from heap
void printKMostFreq( FILE* fp, int k )
{
    // Create a Min Heap of Size k
    MinHeap* minHeap = createMinHeap( k );
    
    // Create an empty Trie
    TrieNode* root = NULL;
 
    // A buffer to store one word at a time
    char buffer[MAX_WORD_SIZE];
 
    // Read words one by one from file.  Insert the word in Trie and Min Heap
    while( fscanf( fp, "%s", buffer ) != EOF )
        insertTrieAndHeap(buffer, &root, minHeap);
 
    // The Min Heap will have the k most frequent words, so print Min Heap nodes
    displayMinHeap( minHeap );
}



int main(int argc, char* argv[])
{	
	int num_procs;
	int rank;
	int j;
	char *topdir, pwd[2]=".";
	char *name_dir= (char*)malloc(100*sizeof(char));
	
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
			
			printf("\n %d I am rank 0 and am waiting for request!",get_lenght(head_process_wait));			
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
				exit(0);
				
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
	char *name_dir = (char*)malloc(100*sizeof(char));
	char *directory_slash=(char*)malloc(2*sizeof(char));
	FILE * fp;
	int k;
	char resolved_path[100]; 
	int count_files=0;
	//char *name_dir;
	struct dirent *entry;
	struct stat statbuf;
	if((dp = opendir(dir)) == NULL) {
        	fprintf(stderr,"cannot open directory: %s\n", dir);
	        return 0;
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
					strcpy(directory_slash,"/");
					//name_dir = name_dir + directory_slash;					
					strcat(name_dir,directory_slash);
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
			/*k = 1;
    			fp = fopen (resolved_path, "r");
    			if (fp == NULL)
        			printf ("File doesn't exist ");
    			else
        			printKMostFreq (fp, 1);

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
