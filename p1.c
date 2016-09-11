    #include <stdio.h>
    #include <assert.h>
    #include <stdlib.h>
    #include <string.h>
    #include<malloc.h>
//    #include<algorithm>	
    #define MAXWORDLEN 128
    
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
        return strcmp(word1->word, word2->word);
    }

    void sortWordList(WordList* wordList)
    {
        qsort(wordList->words, wordList->count, sizeof(Word), compareWord);
    }

    void countWords(const char* text)
    {
        WordList   *wordList = createWordList(0);
        Word       *foundWord = NULL;
        const char *beg = findNonWhitespace(text);
        const char *end;
        char       word[MAXWORDLEN];

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
        printWords(wordList);
    }

int main()
{
FILE *f = fopen("cloud/agency.txt", "r");
fseek(f, 0, SEEK_END);
long fsize = ftell(f);
fseek(f, 0, SEEK_SET);

char *string = malloc(fsize + 1);
fread(string, fsize, 1, f);
fclose(f);

string[fsize] = '\0';
printf("%s",string);
   countWords(string);
}
