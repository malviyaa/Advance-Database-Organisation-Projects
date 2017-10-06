#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>

#include "dberror.h"
#include "storage_mgr.h"
#include "test_helper.h"



FILE *fileptr;

 void initStorageManager (void)
{
    fileptr = NULL;
    printf("Successful initialization of file in storage manager");
}

RC createPageFile (char *fileName)
  {
    fileptr = fopen(fileName,"w+");

    if(fileptr == NULL){
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }

    char s[PAGE_SIZE];

    memset(s,'\0',PAGE_SIZE);

    fwrite(s,PAGE_SIZE,sizeof(char),fileptr);
    fclose(fileptr);
    return 0;  //RC_OK
  }


RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    fileptr = fopen(fileName,"r");
    int numOfPages = 0;
    int readItem = 1;
    char file[PAGE_SIZE];

    if (fileptr == NULL)
    {
        return 1;  //RC_FILE_NOT_FOUND
    }

    while (readItem)
    {
        readItem = fread(file,sizeof(char),PAGE_SIZE,fileptr);
        if (readItem)
        {
            numOfPages = numOfPages+1;
        }
    }

    //printf("                  Total num of pages: %d"           , numOfPages);

    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = numOfPages;
    fHandle->mgmtInfo = fileptr;
    //fclose(fileptr);

    return 0;    //RC_OK
}

RC closePageFile (SM_FileHandle *fHandle)
{
    fileptr = fHandle->mgmtInfo;
    if (fileptr == NULL){
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }
	else
    {
        fclose(fileptr);
        return 0;  //RC_OK
    }
    free(fileptr);
    //fclose(fHandle->mgmtInfo);
}

RC destroyPageFile (char *fileName)
{
	
   int status;
   //unlink(fileName);
   status = remove(fileName);

   //free(fileptr);
   //printf ("                status: %d            ",status);
   	if(status==0)
    {
        return 0;
    }
    return 0;
}


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *filePage;
    fileptr = fHandle->mgmtInfo;
    filePage = fopen(fHandle->fileName, "r");
    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }

    if (pageNum < 0 || pageNum > fHandle->totalNumPages-1)
    {
        return 4;  //RC_READ_NON_EXISTING_PAGE
    }else
    {
        if (fseek(filePage,(pageNum*PAGE_SIZE),SEEK_SET) == 0)
        {
            if (fread(memPage,1,PAGE_SIZE,filePage))
            {
                fHandle->curPagePos = pageNum;
                fclose(filePage);
                return 0;  //RC_OK
            }
        }else
        {
            return 4;  //RC_READ_NON_EXISTING_PAGE
        }
    }
    free(fileptr);
    free(filePage);
    return NULL;
}

int getBlockPos (SM_FileHandle *fHandle)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        int curPagePos;
        curPagePos = fHandle->curPagePos;
        return curPagePos;
    }
}



 RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return readBlock(0,fHandle,memPage);
    }

}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        if (fHandle->curPagePos > 0)
        {
        return readBlock(fHandle->curPagePos-1,fHandle,memPage);
        }else
        return 4;  //RC_READ_NON_EXISTING_PAGE
    }
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return readBlock(fHandle->curPagePos,fHandle,memPage);
    }
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }
    if (fHandle->curPagePos < fHandle->totalNumPages-1)
    {
        return readBlock(fHandle->curPagePos+1,fHandle,memPage);
    }else
    return 4;  //RC_READ_NON_EXISTING_PAGE
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
    }
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    FILE *filePage;
    fileptr = fHandle->mgmtInfo;
    filePage = fopen(fHandle->fileName, "w");
    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }

    if (pageNum < 0 || pageNum > fHandle->totalNumPages-1)
    {
        return 4;  //RC_READ_NON_EXISTING_PAGE
    }else
    {
        if (fseek(filePage,(pageNum*PAGE_SIZE),SEEK_SET) == 0)
        {
            if (fwrite(memPage,1,PAGE_SIZE,filePage))
            {
                fHandle->curPagePos = pageNum;
                fclose(filePage);
                return 0;  //RC_OK
            }
        }else
        {
            return 3;  //RC_WRITE_FAILED
        }
    }
    return NULL;

}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    fileptr = fHandle->mgmtInfo;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return writeBlock(fHandle->curPagePos,fHandle,memPage);
    }
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	fileptr = fHandle->mgmtInfo;
	if(fileptr == NULL)
	{
		return 2;  //RC_FILE_HANDLE_NOT_INIT
	}

	char str[PAGE_SIZE];
	memset(str,'\0',PAGE_SIZE);

	if(!fseek(fileptr, 0, SEEK_END))
	{
		fwrite(str, sizeof(char), PAGE_SIZE, fileptr);
		fHandle->totalNumPages += 1;
		free(fileptr);
		return 0; //RC_OK
	}
	else
	{
		return 3;	//RC_WRITE_FAILED

	}

}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    fileptr = fHandle->mgmtInfo;
    FILE *filePage;
    filePage = fopen(fHandle->fileName, "a");
    RC returnCode;

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        //int i = fHandle->totalNumPages;
        //returnCode = appendEmptyBlock (fHandle);
        //int i = curPagesTotal;
        while (numberOfPages > fHandle->totalNumPages)
            appendEmptyBlock(fHandle);
            //i++;
        fclose(filePage);
        return 0;  //RC_OK
    }
}



