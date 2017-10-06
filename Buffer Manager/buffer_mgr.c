//
// Created by Ankit Malviya on 11/18/16.
//
#include <stdio.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>




typedef struct BM_MInf{
    SM_FileHandle *fileHandle;
    PageNumber *frameContent;
    bool *dirty;
    int *fixCount;
    BM_PageHandle *pageLoc;
    int readCounter;
    int writeCounter;
    int replaceFrame;
    int *LRUCounter;
} BM_MInf;



typedef struct BM_Mgmt{
    BM_MInf *bm_MInf;
}BM_Mgmt;


BM_Mgmt *mgmt = NULL;

/****************************************************************************
 * Function Name: initBufferPool
 * 			Via this function, Buffer Pool is initialized and initial values for the variables are defined
 * Arguments:
 *			pointer to bm
 *			pointer to pageFileName
 *			numPages
 *			strategy
 *			pointer to stratData
 * Return:
 * 			RC returned code
 *****************************************************************************/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData){

    mgmt = (mgmt == NULL ? MAKE_MGMT_INFO():mgmt);

    SM_FileHandle *fh = malloc(sizeof(SM_FileHandle));
    RC code;


    int i;
    PageNumber frame = -1;

    int sizeBufferMgmtInfo = sizeof(BM_MInf);
    int sizePageNum = sizeof(PageNumber);
    int sizePageHandle = sizeof(BM_PageHandle);

    //BM_BufferPool_MgmtInfo *bm_MInf = malloc(sizeBufferMgmtInfo);
    mgmt->bm_MInf = malloc(sizeBufferMgmtInfo);
    mgmt->bm_MInf->dirty = (bool *) malloc (numPages);
    mgmt->bm_MInf->fixCount = (int *) malloc (4 * numPages);
    mgmt->bm_MInf->pageLoc = (BM_PageHandle *) malloc (sizePageHandle * numPages);
    mgmt->bm_MInf->LRUCounter = (int *) malloc (4 * numPages);
    code = openPageFile (pageFileName, fh);

    if (code == RC_OK){
        bm->pageFile = pageFileName;
        bm->numPages = numPages;
        bm->strategy = strategy;
        i = 0;
        while(numPages > i)
        {
            mgmt->bm_MInf->frameContent [i] = frame;
            mgmt->bm_MInf->dirty [i] = false;
            mgmt->bm_MInf->fixCount [i] = 0;
            mgmt->bm_MInf->LRUCounter [i] = -1;
            i += 1;
        }
        mgmt->bm_MInf->fileHandle = fh;
        mgmt->bm_MInf->readCounter = 0;
        mgmt->bm_MInf->writeCounter = 0;
        mgmt->bm_MInf->replaceFrame = -1;
        bm->mgmtData = mgmt->bm_MInf;
    }

    return code;

}


/*********************************************************************************
 * Function Name: shutDownBufferPool
 * 			Via this function, BufferPool is destroyed and all the resources are freed
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC shutdownBufferPool(BM_BufferPool *const bm){
    RC Code;
    mgmt->bm_MInf = (BM_MInf*)bm->mgmtData;
    SM_FileHandle *fh = (SM_FileHandle *)mgmt->bm_MInf->fileHandle;
    Code = forceFlushPool(bm);
    if(Code != RC_OK)
    {
        free(mgmt->bm_MInf);

        return Code;

    }
    Code = closePageFile (fh);
    free(mgmt->bm_MInf);
    return Code;
}


/*********************************************************************************
 * Function Name - forceFlushPool
 * 			Via this function, all dirty pages are written from BufferPool to disk
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC forceFlushPool(BM_BufferPool *const bm)
{
    RC Code = RC_OK;

    int i, pages;

    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;

    int sizePageHandle = sizeof(BM_PageHandle);

    BM_PageHandle *ph = malloc(sizePageHandle);
    SM_FileHandle *fh = (SM_FileHandle *)mgmt->bm_MInf->fileHandle;

    pages = bm->numPages;
    i=0;

    while(pages > i)
    {
        if (mgmt->bm_MInf->dirty [i] == true)

            if(mgmt->bm_MInf->fixCount[i] == 0)

            {
                *ph = mgmt->bm_MInf->pageLoc [i];
                Code = writeBlock (mgmt->bm_MInf->frameContent [i], fh, ph->data);
                if (Code == RC_OK)
                {
                    mgmt->bm_MInf->dirty [i] = false;
                    mgmt->bm_MInf->writeCounter+=1;
                }
            }

        i = i+1;

    }



    return Code;

}


/*********************************************************************************
 * Function Name: markDirty
 * 			Via this function, page is marked dirty
 * Arguments:
 *			pointer to bm
 *			pointer to page
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{


    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;

    int i = 0;


    while(bm->numPages > i)
    {
        if(page->pageNum == mgmt->bm_MInf->frameContent[i])
        {
            mgmt->bm_MInf-> dirty [i] = 1;
            break;
        }
        i++;
    }

    return RC_OK;
}


/*********************************************************************************
 * Function Name: unpinPage
 * 			Via this function, page is unpinned from BufferPool
 * Arguments:
 *			pointer to bm
 *			pointer to page
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;


    int i = 0;
    int result = 0;
    while(bm->numPages > i)
    {
        if (mgmt->bm_MInf->frameContent [i] == page->pageNum)
            if(mgmt->bm_MInf->fixCount > 0)
            {
                result = 1;

                break;
            }
        i++;
    }
    mgmt->bm_MInf->fixCount [i] = (result == 1 ? mgmt->bm_MInf->fixCount [i] - 1 : mgmt->bm_MInf->fixCount [i]);

    return RC_OK;

}


/*********************************************************************************
 * Function Name: forcePage
 * 			Via this function, content of the page is written on disk
 * Arguments:
 *			pointer to bm
 *			pointer to page
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;


    SM_FileHandle *fh;
    int i = 0;

    while(bm->numPages > i){
        if(mgmt->bm_MInf->frameContent [i] == page->pageNum)
            if(mgmt->bm_MInf->fixCount[i] == 0)
                if(mgmt->bm_MInf->dirty [i] == 1)
                {
                    openPageFile(bm->pageFile, fh);
                    writeBlock(mgmt->bm_MInf->frameContent[i], fh, page->data);
                    closePageFile(fh);

                    mgmt->bm_MInf->dirty [i] = 0;

                    mgmt->bm_MInf->writeCounter+=1;
                }
        i = i+1;
    }

    return RC_OK;
}


/*********************************************************************************
 * Function Name: pinPage
 * 			Via this function, page is pinned and either of the strategy FIFO or LRU is used
 * Arguments:
 *			pointer to bm
 *			pointer to page
 *			pageNum
 * Return:
 * 			RC returned code
 *********************************************************************************/

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum){
    RC code = RC_OK;
    int i, j, pages, frameReplace = -1, pageInMem = 0,next, useLRU;
    pages = bm->numPages;
    next = mgmt->bm_MInf->replaceFrame;

    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;
    SM_FileHandle *fh = (SM_FileHandle *)mgmt->bm_MInf->fileHandle;
    int sizePageHandle = sizeof(BM_PageHandle);
    BM_PageHandle *ph = malloc(sizePageHandle);
    SM_PageHandle memPage = malloc(sizePageHandle);

    i = 0;
    while(i < pages)
    {
        if (mgmt->bm_MInf->frameContent [i] == pageNum){
            *page = mgmt->bm_MInf->pageLoc [i];
            mgmt->bm_MInf->fixCount [i]++;
            useLRU = 0;
            if(bm->strategy == RS_LRU){

                j = 0;
                while(j < pages -1)
                {
                    useLRU = (mgmt->bm_MInf->LRUCounter [j] == i ? 1:useLRU);

                    mgmt->bm_MInf->LRUCounter [j] = (useLRU == 1 ? mgmt->bm_MInf->LRUCounter [j+1]:mgmt->bm_MInf->LRUCounter [j]);

                    j= j +1;
                }
                mgmt->bm_MInf->LRUCounter [pages - 1] = -1;
                j = 0;
                while(j < pages)
                {
                    if (mgmt->bm_MInf->LRUCounter [j] == -1){
                        mgmt->bm_MInf->LRUCounter [j] = i;
                        j = pages;
                    }
                    j = j + 1;
                }
                //break;
            }
            pageInMem = 1;
            i = pages;
        }
        i = i+1;
    }

    if (pageInMem == 0){

        i = 0;
        while(i < pages)
        {

            if (mgmt->bm_MInf->frameContent [i] == -1){
                frameReplace = i;
                break;
            }
            i = i + 1;
        }


        if(frameReplace == -1)
        {
            if(bm->strategy == RS_FIFO)
            {


                do {
                    next += 1;
                    next = ((next == pages) ? 0:next);

                    mgmt->bm_MInf->replaceFrame = next;
                }while(mgmt->bm_MInf->fixCount [next] > 0);
                frameReplace = mgmt->bm_MInf->replaceFrame;
            }
            else if(bm->strategy == RS_LRU){
                frameReplace = mgmt->bm_MInf->LRUCounter [0];
                i = 0;
                while(i < pages - 1)
                {
                    mgmt->bm_MInf->LRUCounter [i] = mgmt->bm_MInf->LRUCounter [i+1];
                    i++;
                }
                mgmt->bm_MInf->LRUCounter [pages - 1] = -1;
            }
            else
                printf("\nStrategy not implemented\n");


        }

        if (frameReplace == -1){

            code = RC_READ_NON_EXISTING_PAGE;
        }
        else{
            bool result;
            result = mgmt->bm_MInf->dirty [frameReplace];
            if (result == true){
                *ph = mgmt->bm_MInf->pageLoc [frameReplace];
                code = writeBlock (mgmt->bm_MInf->frameContent [frameReplace], fh, ph->data);
                mgmt->bm_MInf->writeCounter = (code == RC_OK ? mgmt->bm_MInf->writeCounter+1 : mgmt->bm_MInf->writeCounter);
            }
            code = (pageNum >= fh->totalNumPages ? ensureCapacity (pageNum + 1, fh) : code);

            code = readBlock (pageNum, fh, memPage);
            if (code == RC_OK){
                page->pageNum = pageNum;
                page->data = memPage;
                mgmt->bm_MInf->fixCount [frameReplace] += 1;
                mgmt->bm_MInf->frameContent [frameReplace] = pageNum;
                mgmt->bm_MInf->dirty [frameReplace] = false;
                mgmt->bm_MInf->pageLoc [frameReplace] = *page;
                mgmt->bm_MInf->readCounter+=1;
                if(bm->strategy == RS_LRU)
                {	i = 0;
                    while(pages > i)
                    {
                        if (mgmt->bm_MInf->LRUCounter [i] == -1)
                        {
                            mgmt->bm_MInf->LRUCounter [i] = frameReplace;
                            break;
                        }
                        i = i+1;
                    }
                }
            }
        }
    }
    return code;
}


/*********************************************************************************
 * Function Name: getFrameContents
 * 	                Via this function, array of page numbers is returned where count maintains number of pages stored in each frame
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			PageNumber
 *********************************************************************************/

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;
    return mgmt->bm_MInf->frameContent;
}


/*********************************************************************************
 * Function Name: getDirtyFlags
 * 	                Via this function, array of bool is returned that contains value True if page stored in page-frame is dirty
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			Boolean
 *********************************************************************************/

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;
    return mgmt->bm_MInf->dirty;
}


/*********************************************************************************
 * Function Name: getFixCounts
 * 	                Via this function, array of ints (of size numPages) is returned that consists of fix counts of pages stored in frame
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			Integer
 *********************************************************************************/

int *getFixCounts (BM_BufferPool *const bm)
{
    mgmt->bm_MInf =(BM_MInf *)bm->mgmtData;
    return mgmt->bm_MInf->fixCount;
}


/*********************************************************************************
 * Function Name: getNumReadIO
 * 	                Via this function, number of pages is returned that've been read from the disk since the Buffer Pool is initialized
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			Integer
 *********************************************************************************/

int getNumReadIO (BM_BufferPool *const bm)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;
    return mgmt->bm_MInf->readCounter;
}


/*********************************************************************************
 * Function Name: getNumWriteIO
 * 	              Via this function, number of pages is returned that've been written to the page file since the Buffer Pool is initialized
 * Arguments:
 * 			pointer to bm
 * Return:
 * 			Integer
 *********************************************************************************/

int getNumWriteIO (BM_BufferPool *const bm)
{
    mgmt->bm_MInf = (BM_MInf *)bm->mgmtData;
    return mgmt->bm_MInf->writeCounter;
}
