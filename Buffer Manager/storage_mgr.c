#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "storage_mgr.h"
#include "dberror.h"




FILE *fileName_var;
char file[PAGE_SIZE]; //This is the pointer to a block of memory with a minimum size of PAGE_SIZE*[] bytes.


//**********************************************************************
void initStorageManager(void)
{
printf("\n Storage Manager is initialized*****************");
}

RC createPageFile (char *fileName){
    int i;

    char *s= malloc(PAGE_SIZE * sizeof(char));

   fileName_var= (FILE *)fopen(fileName,"wb");

    if (fileName_var == NULL)
        return 2;  //RC_FILE_HANDLE_NOT_INIT;


        fwrite(s,sizeof(char),PAGE_SIZE,fileName_var);
       // fclose(fileName_var);

        return 0;  //RC_OK;

}


//**********************************************************************

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    fileName_var = fopen(fileName, "r+b");
    int numOfPages = 0;
    int curPagePos=0;
  //  int readItem ;
    int items,status = 1;

    if (fileName_var == NULL)
        return RC_FILE_NOT_FOUND;

    while (status==1)
    {
        status = fread(file,sizeof(char),PAGE_SIZE,fileName_var);
        if (status)
        {
            numOfPages = numOfPages+1;
        }
    }
  //  update_fHandle(fHandle,fileName,numOfPages,curPagePos,fileName);
    fHandle->fileName = fileName;
    fHandle->totalNumPages = numOfPages;
    fHandle->curPagePos = curPagePos;
    fHandle->mgmtInfo = fileName;

        return 0;  //RC_OK;
    }

//void update_fHandle( SM_FileHandle *fHandle, char *fileName, int totalNumPages, int curPagePos, void *mgmtInfo)
//{
//    fHandle->fileName = fileName;
//    fHandle->totalNumPages = totalNumPages;
//    fHandle->curPagePos = curPagePos;
//    fHandle->mgmtInfo = fileName;
//}



//**********************************************************************

RC closePageFile (SM_FileHandle *fHandle){


    if (fileName_var == NULL){
        return 2;  //RC_FILE_HANDLE_NOT_INIT;  //RC_FILE_HANDLE_NOT_INIT
    }
    else
    {
        fclose(fileName_var);
        return 0;  //RC_OK;  //RC_OK
    }

}

//**********************************************************************

RC destroyPageFile (char *fileName){

    RC status;
    fclose(fileName_var);
    status = remove(fileName);
    if(status==0)
        return 0;  //RC_OK;
}

//**********************************************************************

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

    int freadIndex;

    if (fileName_var == NULL){
        return 2;  //RC_FILE_HANDLE_NOT_INIT;  //RC_FILE_HANDLE_NOT_INIT
    }

    fseek(fileName_var,pageNum*PAGE_SIZE,SEEK_SET);
    freadIndex = fread(memPage,PAGE_SIZE,sizeof(char),fileName_var);

    fHandle->curPagePos = (freadIndex == 1 ? pageNum:fHandle->curPagePos);
    int result;
    result = (fHandle->curPagePos!=pageNum ? 4:0);

  //  update_fHandle(fHandle,fileName,numOfPages,curPagePos,fileName);
    return result;
}


//**********************************************************************

int getBlockPos (SM_FileHandle *fHandle){


    if(fileName_var == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        int curPagePos;
        curPagePos = fHandle->curPagePos;
        return curPagePos;
    }
}

//**********************************************************************


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){


int readIndex;

    if(fileName_var == NULL)
        return 2;  //RC_FILE_HANDLE_NOT_INIT

    else if(fileName_var<=0)
        return 1;   //RC_FILE_NOT_FOUND

    else

        return readBlock(0,fHandle,memPage);

    if (readIndex==1)
    {
        fHandle->curPagePos=0;
        return  0;  //RC OK
    } else
        return 4; // RC_READ_NON_EXISTING_PAGE


}


//**********************************************************************

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    int previous_page_position;
    if(fileName_var == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        if (fHandle->curPagePos > 0)
        {
            previous_page_position= (fHandle->curPagePos-1);
            fseek(fileName_var,previous_page_position*PAGE_SIZE,SEEK_SET);

            return readBlock(previous_page_position,fHandle,memPage);
        }else
            return 4;  //RC_READ_NON_EXISTING_PAGE
    }
}

//**********************************************************************


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){


    if(fileName_var == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return readBlock(fHandle->curPagePos,fHandle,memPage);
    }
}


//**********************************************************************

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){


    if(fileName_var == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }
    if (fHandle->curPagePos < fHandle->totalNumPages-1)
    {
        return readBlock(fHandle->curPagePos+1,fHandle,memPage);
    }else
        return 4;  //RC_READ_NON_EXISTING_PAGE
}

//********************************************************************************************************/

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){


    if(fileName_var == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {
        return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
    }
}


//********************************************************************************************************/

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

    if(fileName_var == NULL)
        return 2;
    if (fHandle->totalNumPages < pageNum && ensureCapacity(pageNum, fHandle) == RC_OK)
        return 3;  //RC_WRITE_FAILED;



    fseek(fileName_var,pageNum*PAGE_SIZE,SEEK_SET);

    int result;
    result = (fwrite(memPage,PAGE_SIZE,sizeof(char),fileName_var) == 1 ? 0:3);
    return result;

}

//********************************************************************************************************/

RC appendEmptyBlock (SM_FileHandle *fHandle){




    if (fileName_var == NULL)
        return 2;  //RC_FILE_HANDLE_NOT_INIT;
    else{
        char *s= malloc(PAGE_SIZE * sizeof(char));



        fseek(fileName_var,0,SEEK_END);

        if (fwrite(s,PAGE_SIZE,sizeof(char),fileName_var) == 1) {
            fHandle->curPagePos = fHandle->totalNumPages;
            fHandle->totalNumPages = (fHandle->totalNumPages+1);
            return 0;  //RC_OK;
        }
        else{
            return 3;  //RC_WRITE_FAILED;
        }
    }
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
    FILE *filePage = (FILE *)fopen(fHandle->fileName, "a");
    //filePage = fopen(fHandle->fileName, "a");

    if(fileptr == NULL)
    {
        return 2;  //RC_FILE_HANDLE_NOT_INIT
    }else
    {

        while (numberOfPages > fHandle->totalNumPages)
            appendEmptyBlock(fHandle);

        fclose(filePage);
        return 0;  //RC_OK
    }
}

//
//
//
//
//
//
//
//
//void initStorageManager(void){
//}
//
///*******************************************************************************************************
//*Function: createPageFile
//	Create a new page file fileName. The initial file size is a one page. This function fills single page
//	with '\0' bytes.
//
//*Arguments:
//	pointer to fileName
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC createPageFile (char *fileName){
//    FILE *fileptr = (FILE *)fopen(fileName, "wb");
//    int i;
//    char s[PAGE_SIZE];
//
//    if (fileptr == NULL)
//        return 2;  //RC_FILE_HANDLE_NOT_INIT;
//    else{
//        while(i < PAGE_SIZE)
//        {   i = i+1;
//            s[i] = '\0';
//        }
//
//        fwrite(s,PAGE_SIZE,sizeof(char),fileptr);
//        fclose(fileptr);
//
//        return 0;  //RC_OK;
//    }
//}
//
//
///*******************************************************************************************************
//*Function: openPageFile
//	Opens an existing page file.
//
//*Arguments:
//	pointer to fileName
//	pointer to fHandle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC openPageFile (char *fileName, SM_FileHandle *fHandle){
//    FILE *fileptr = (FILE *)fopen(fileName, "r+b");
//    int items,status = 1;
//    char file[PAGE_SIZE];
//
//    if (fileptr == NULL)
//        return RC_FILE_NOT_FOUND;
//    else{
//        items = 0;
//        while(status == 1){
//            status = fread(file,PAGE_SIZE,sizeof(char),fileptr);
//            items = (status == 1 ? items+1 : items);
//
//        }
//
//        fHandle->fileName = fileName;
//        fHandle->totalNumPages = items;
//        fHandle->curPagePos = 0;
//        fHandle->mgmtInfo = fileptr;
//
//        return 0;  //RC_OK;
//    }
//}
//
//
///*******************************************************************************************************
//*Function: closePageFile
//	Close an open page file.
//
//*Arguments:
//	pointer to fHandle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC closePageFile (SM_FileHandle *fHandle){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if (fileptr == NULL){
//        return 2;  //RC_FILE_HANDLE_NOT_INIT;  //RC_FILE_HANDLE_NOT_INIT
//    }
//    else
//    {
//        fclose(fileptr);
//        return 0;  //RC_OK;  //RC_OK
//    }
//    free(fileptr);
//}
//
//
///*******************************************************************************************************
//*Function: destroyPageFile
//	Dstroy an open page file.
//
//*Arguments:
//	pointer to fileName
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC destroyPageFile (char *fileName){
//    RC status;
//    status = remove(fileName);
//    if(status==0)
//        return 0;  //RC_OK;
//}
//
//
///*******************************************************************************************************
//*Function: readBlock
//	The method reads the pageNumth block from a file and stores its content in the memory pointed to by
//	the memPage page handle.
//
//*Arguments:
//	int pageNum
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//    int freadIndex;
//
//    fseek(fileptr,pageNum*PAGE_SIZE,SEEK_SET);
//    freadIndex = fread(memPage,PAGE_SIZE,sizeof(char),fileptr);
//    fHandle->curPagePos = (freadIndex == 1 ? pageNum:fHandle->curPagePos);
//    int result;
//    result = (fHandle->curPagePos!=pageNum ? 4:0);
//    return result;
//}
//
//
///*******************************************************************************************************
//*Function: getBlockPos
//	The method returns the current page position in a file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code if failure
//	Or current page position.
//
//********************************************************************************************************/
//
//int getBlockPos (SM_FileHandle *fHandle){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        int curPagePos;
//        curPagePos = fHandle->curPagePos;
//        return curPagePos;
//    }
//}
//
//
///*******************************************************************************************************
//*Function: readFirstBlock
//	This function reads the first page in the file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//    fileptr = fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        return readBlock(0,fHandle,memPage);
//    }
//
//}
//
//
///*******************************************************************************************************
//*Function: readPreviousBlock
//	Read the previous page relative to the curPagePos of the file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        if (fHandle->curPagePos > 0)
//        {
//            return readBlock(fHandle->curPagePos-1,fHandle,memPage);
//        }else
//            return 4;  //RC_READ_NON_EXISTING_PAGE
//    }
//}
//
//
///*******************************************************************************************************
//*Function: readCurrentBlock
//	Read the current page relative to the curPagePos of the file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        return readBlock(fHandle->curPagePos,fHandle,memPage);
//    }
//}
//
//
///*******************************************************************************************************
//*Function: readNextBlock
//	Read the next page relative to the curPagePos of the file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }
//    if (fHandle->curPagePos < fHandle->totalNumPages-1)
//    {
//        return readBlock(fHandle->curPagePos+1,fHandle,memPage);
//    }else
//        return 4;  //RC_READ_NON_EXISTING_PAGE
//}
//
//
///*******************************************************************************************************
//*Function: readLastBlock
//	Read the last page in file.
//
//*Arguments:
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        return readBlock(fHandle->totalNumPages-1,fHandle,memPage);
//    }
//}
//
//
///*******************************************************************************************************
//*Function: writeBlock
//	This function writes data in the page number provided as input in the file initialized in file handle
//*Arguments:
//	int pageNum
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//    if(fileptr == NULL)
//        return 2;
//    if (fHandle->totalNumPages < pageNum && ensureCapacity(pageNum, fHandle) == RC_OK)
//        return 3;  //RC_WRITE_FAILED;
//
//
//
//    fseek(fileptr,pageNum*PAGE_SIZE,SEEK_SET);
//
//    int result;
//    result = (fwrite(memPage,PAGE_SIZE,sizeof(char),fileptr) == 1 ? 0:3);
//    return result;
//
//}
//
//
///*******************************************************************************************************
//*Function: writeCurrentBlock
//	This function writes data in the page number provided as input in the file initialized in file handle
//*Arguments:
//	int pageNum
//	pointer to fHandle
//	memPage is a page handle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
//
//
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    if(fileptr == NULL)
//    {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    }else
//    {
//        return writeBlock(fHandle->curPagePos,fHandle,memPage);
//    }
//}
//
//
///*******************************************************************************************************
//*Function: appendEmptyBlock
//This function appends an empty block in the end of file initialized in file handle.
//
//*Arguments:
//	pointer to fHandle
//
//*Return:
//	RC return code.
//
//********************************************************************************************************/
//
//RC appendEmptyBlock (SM_FileHandle *fHandle){
//    FILE *fileptr = (FILE *)fHandle->mgmtInfo;
//
//    int i = 0;
//    int lstPg;
//    char s[PAGE_SIZE];
//
//    if (fileptr == NULL)
//        return 2;  //RC_FILE_HANDLE_NOT_INIT;
//    else{
//        while(i < PAGE_SIZE)
//        {
//            s[i] = '\0';
//            i++;
//        }
//
//        lstPg = fHandle->totalNumPages;
//
//        fseek(fileptr,lstPg*PAGE_SIZE,SEEK_SET);
//
//        if (fwrite(s,PAGE_SIZE,sizeof(char),fileptr) == 1) {
//            fHandle->curPagePos = lstPg;
//            fHandle->totalNumPages = lstPg + 1;
//            return 0;  //RC_OK;
//        }
//        else{
//            return 3;  //RC_WRITE_FAILED;
//        }
//    }
//}
//
//
///*******************************************************************************************************
//*Function: ensureCapacity
//
//	This function increases the size of numberOfPages if the file has less than numberOfPages pages.
//
//*Arguments:
//	int numberOfPages
//	pointer to fHandle
//
//*Return:
//	RC return code.
//********************************************************************************************************/
//
//RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
//    FILE *fileptr = (FILE *) fHandle->mgmtInfo;
//    FILE *filePage = (FILE *) fopen(fHandle->fileName, "a");
//    //filePage = fopen(fHandle->fileName, "a");
//
//    if (fileptr == NULL) {
//        return 2;  //RC_FILE_HANDLE_NOT_INIT
//    } else {
//
//        while (numberOfPages > fHandle->totalNumPages)
//            appendEmptyBlock(fHandle);
//
//        fclose(filePage);
//        return 0;  //RC_OK
//    }
//}