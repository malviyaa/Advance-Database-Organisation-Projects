#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>
#include<unistd.h>
#include"storage_mgr.h"
#include"buffer_mgr.h"
#include"record_mgr.h"
#include"tables.h"
#include"definition.h"

void updateRecordInfo(int pageNumber,int slotNumber);

typedef struct dataStoreNode {
    struct dataStoreNode *next;
    RID id;
}dataStoreNode;

typedef struct recordTableInfo{
    BM_BufferPool *bufferManager;
    int maxPgSlt;
    int numTuples;
    int sizeofSchema;
    int dataStoreLength;
    dataStoreNode *dataStoreHead;
    int recordStart;
    int recordEnd;
    int sizeOfSlot;
    int dataStoreInfoListSize;
}recordTableInfo;

typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;



typedef struct recordSearch {
    int pagesPerRelation;
    int slotsPerRelation;
    Expr *condition;
    int currRecSlotNo;
    int currRecPgNo;
}recordSearch;


typedef struct RM_Mgmt
{
	recordTableInfo *rtf;
	VarString *varStr;
	recordSearch *srchRecord;
	dataStoreNode *dsNode;
	Schema *sch;
	Record  *rec;
	RM_TableData *td;
	RM_ScanHandle *sh;
	Value *value;

}RM_Mgmt;




RM_Mgmt *mgt = NULL;


recordTableInfo *getRecMgrTableInfo(RM_TableData *rel){
	mgt->rtf = (RM_Mgmt *) (rel->mgmtData);
    return mgt->rtf;
}


dataStoreNode *getDataStoreNode(){
	mgt->dsNode = (RM_Mgmt *)calloc(sizeof(RM_Mgmt),sizeof(RM_Mgmt));
	mgt->dsNode->next = NULL;
    return mgt->dsNode;
}


int processDataStore(recordTableInfo *recordTable){
	mgt->dsNode = recordTable->dataStoreHead;
    int dataStoreCount = 0,i=0;
    while(mgt->dsNode != NULL)
    {
    	mgt->dsNode = mgt->dsNode->next;
        dataStoreCount++;
        i++;
    }
    return dataStoreCount;
}


char *strTableMaker(recordTableInfo *recordTable,VarString *varString){
	mgt->dsNode = recordTable->dataStoreHead;
    char *opStr;
    int i = 0;
    while(mgt->dsNode != NULL)
    {
        APPEND(varString,"%i:%i%s ",mgt->dsNode->id.page, mgt->dsNode->id.slot, (mgt->dsNode->next != NULL) ? ", ": "");
        mgt->dsNode = mgt->dsNode->next;
    }
    APPEND_STRING(varString, ">\n");				\
			GET_STRING(opStr, varString);
    return opStr;
}


char *tableToStr(recordTableInfo *recordTable){
    char *opStr;
    VarString *varString;
    MAKE_VARSTRING(varString);
    APPEND(varString, "SchemaLength <%i> FirstRecordPage <%i> LastRecordPage <%i> NumTuples <%i> SlotSize <%i> MaxSlots <%i> ", recordTable->sizeofSchema, recordTable->recordStart, recordTable->recordEnd, recordTable->numTuples, recordTable->sizeOfSlot, recordTable->maxPgSlt);
    int dataStoreCount =processDataStore(recordTable);
    APPEND(varString, "tNodeLen <%i> <", dataStoreCount);
    opStr=strTableMaker(recordTable,varString);
    return opStr;

}


recordTableInfo *initRecMgrTableInfo(){
	mgt->dsNode = (RM_Mgmt*) malloc(sizeof(RM_Mgmt));
    return mgt->dsNode;
}


void strTokenize(char **s1){
    *s1 = strtok (NULL,"<");
    *s1 = strtok (NULL,">");
}

long int startExt(char **s1,char **s2){
    long int extData=strtol((*s1), &(*s2), 10);
    return extData;
}


void processDataForDataStore(recordTableInfo **recordTable,int pageNumber,int slotNumber){

	if (mgt->rtf->dataStoreHead != NULL){
    	mgt->dsNode->next = getDataStoreNode();
    	mgt->dsNode->next->id.page = pageNumber;
    	mgt->dsNode->next->id.slot = slotNumber;
    	mgt->dsNode = mgt->dsNode->next;
    }
    else{
    	mgt->rtf->dataStoreHead = getDataStoreNode();
    	mgt->rtf->dataStoreHead->id.page = pageNumber;
    	mgt->rtf->dataStoreHead->id.slot = slotNumber;
        mgt->dsNode = mgt->rtf->dataStoreHead;
    }
}


recordTableInfo *processDStore(recordTableInfo *recordTable,char **s1,char **s2){
    int k, pageNumber, slotNumber;
    mgt->rtf->dataStoreHead = NULL;
    while(mgt->rtf->dataStoreLength>k){
        *s1 = strtok (NULL,":");
        pageNumber = startExt(&(*s1),&(*s2));

        *s1 = ((k == (mgt->rtf->dataStoreLength - 1))?strtok (NULL,">"):strtok (NULL,","));
        /*
        if(k != (mgt->rtf->dataStoreLength - 1)){
            *s1 = strtok (NULL,",");
        }
        else{
            *s1 = strtok (NULL,">");
        }
        */
        slotNumber = startExt(&(*s1),&(*s2));
        processDataForDataStore(&recordTable,pageNumber,slotNumber);
        k++;
    }
    return recordTable;
}


recordTableInfo *StrToRecTable(char *recordTableInfoString){
	int len = strlen(recordTableInfoString);
    char recordTableData[len];
    strcpy(recordTableData, recordTableInfoString);
    mgt->rtf = initRecMgrTableInfo();
    char *s1, *s2;
    s1 = strtok (recordTableData,"<");
    s1 = strtok (NULL,">");
    mgt->rtf->sizeofSchema = startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->recordStart = startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->recordEnd =  startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->numTuples =  startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->sizeOfSlot =  startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->maxPgSlt =  startExt(&s1,&s2);
    strTokenize(&s1);
    mgt->rtf->dataStoreLength = startExt(&s1,&s2);
    mgt->rtf->dataStoreInfoListSize = startExt(&s1,&s2);
    s1 = strtok (NULL,"<");
    mgt->rtf=processDStore(mgt->rtf,&s1,&s2);
    return mgt->rtf;
}


Schema *getSchema(){
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    return schema;
}

void schemaManupulate(char **s1,char *schemaData){
    *s1 = strtok (schemaData,"<");
    *s1 = strtok (NULL,">");
}

int getNumOfAttrs(char **s1,char **s2){
    int numOfAttrs = strtol(*s1, &(*s2), 10);
    return numOfAttrs;
}


void schemaPopulate(int numOfAttrs){
    mgt->sch->typeLength=(int *)malloc(sizeof(int)*numOfAttrs);
    mgt->sch->attrNames=(char **)malloc(sizeof(char*)*numOfAttrs);
    mgt->sch->numAttr= numOfAttrs;
    mgt->sch->dataTypes=(DataType *)malloc(sizeof(DataType)*mgt->sch->numAttr);
}

void schemaByattributeName(char **s1,int i){
	mgt->sch->attrNames[i]=(char *)calloc(strlen(*s1), sizeof(char));
    strcpy(mgt->sch->attrNames[(i)], *s1);
    *s1 = ((i == mgt->sch->numAttr-1)?strtok (NULL,") "):strtok (NULL,", "));
}


bool dataTypesPopulate(char **s1,int i){
    bool flag=true;
    if (strcmp(*s1, "FLOAT") == 0){
        mgt->sch->typeLength[i] = 0;
        mgt->sch->dataTypes[i] = DT_FLOAT;
        flag=false;
    } else if (strcmp(*s1, "BOOL") == 0){
    	mgt->sch->typeLength[i] = 0;
    	mgt->sch->dataTypes[i] = DT_BOOL;
        flag=false;
    }else if (strcmp(*s1, "INT") == 0){
    	mgt->sch->typeLength[i] = 0;
    	mgt->sch->dataTypes[i] = DT_INT;
        flag=false;
    }
    return flag;
}
char *extract(char **s1){
    *s1 = strtok (NULL,")");
    char *main = strtok (*s1,", ");
    return main;
}

char extSq(char **s1,char **s3){
    *s1 = strtok (*s3,"[");
    *s1 = strtok (NULL,"]");
}


Schema *schemaDeslrzd(char *stringSchema,char temp,int count){
    int index, index1, keySize = 0;
    char *s1, *s2,*s3;
    char schemaData[strlen(stringSchema)];
    strcpy(schemaData, stringSchema);
    schemaManupulate(&s1,schemaData);
    mgt->sch = getSchema();
    int numOfAttrs=getNumOfAttrs(&s1,&s2);
    char* Attributes[numOfAttrs];
    schemaPopulate(numOfAttrs);
    char* stringRef[numOfAttrs];
    s1 = strtok (NULL,"(");
    bool flag;
    for(index =0;index<mgt->sch->numAttr;index++){
        s1 = strtok (NULL,": ");
        schemaByattributeName(&s1,index);
        stringRef[index] = (char)calloc(strlen(s1), sizeof(char));
        if(flag==(dataTypesPopulate(&s1,index))){
            strcpy(stringRef[index], s1);

        }else{

        }

    }
    flag = false;
    if((s1 = strtok (NULL,"(")) != NULL){
        flag = true;
        char *main=extract(&s1);
        index = 0;
        while(main != NULL)
        {
        	Attributes[keySize] = (char)calloc(strlen(main), sizeof(char));
            strcpy(Attributes[keySize], main);
            main = strtok (NULL,", ");
            keySize+=1;
            index++;
        }
    }
    for(index=0;index<numOfAttrs;index++)
    {
        if(strlen(stringRef[index]) > 0){
        	mgt->sch->dataTypes[index] = DT_STRING;
            s3 = (char)calloc(strlen(stringRef[index]), sizeof(char));
            memcpy(s3, stringRef[index], strlen(stringRef[index]));
            extSq(&s1,&s3);
            mgt->sch->typeLength[index] = getNumOfAttrs(&s1,&s2);

        }
    }
    if(flag == true){
    	mgt->sch->keyAttrs=(int *)malloc(sizeof(int)*keySize);
    	mgt->sch->keySize = keySize;
        for(index = 0; index<keySize; index++)
        {
            for(index1=0;index1<mgt->sch->numAttr;index1++)
            {
            	mgt->sch->keyAttrs[index] = ((strcmp(Attributes[index], mgt->sch->attrNames[index1]) == 0)?index1:mgt->sch->keyAttrs[index]);

            }

        }
        return mgt->sch;
    }else{
        return mgt->sch;
    }
}


int getNumTuples (RM_TableData *rel){
    return ((recordTableInfo *)rel->mgmtData)->numTuples;
}


RC freeRecord (Record *record){
    free(record->data);
    free(record);
    return RC_OK;
}


RC openingFile(char *name,SM_FileHandle fHandle){
    RC flag;
    flag = (((flag=openPageFile(name, &fHandle)) != RC_OK)?flag:flag);
    return flag;
}


RC writeToFile(SM_FileHandle fHandle,recordTableInfo *recordTable){
    char *recordTableString = tableToStr(recordTable);
    RC flag;
    flag = (((flag=writeBlock(0, &fHandle, recordTableString)) != RC_OK)?flag:flag);
    free(recordTableString);
    return flag;
}


RC closeFile(SM_FileHandle fHandle){
    RC flag;
    flag = (((flag=closePageFile(&fHandle)) != RC_OK)?flag:flag);

    return flag;
}


bool tableExistCheck(char *name){
	bool result;
	result = ((access(name, 0) == -1)?false:true);
	return result;
}


RC tableInfoToFile(char *name,char *temp){
    SM_FileHandle fHandle;
    RC flag;

    if(!tableExistCheck(name)) {
        RC_message="Table not found";
        return TABLE_DOES_NOT_EXIST;
    }

    flag=openPageFile(name,&fHandle);
    if(openPageFile(name,&fHandle)==RC_OK){
        char *recordTableString = tableToStr(mgt->rtf);
        flag=writeBlock(0,&fHandle,recordTableString);
        if(flag==RC_OK){
            flag=closePageFile(&fHandle);
            if(flag==RC_OK){
                return RC_OK;
            }else{
                return flag;
            }

        }else{
            return flag;
        }
    }else{
        return flag;
    }
}


void pgOpn(int pageNumber,int slotNumber,RM_TableData *rel){

    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));    //before writing record to pageFile serialize the record to be written
    char *rString = serializeRecord(mgt->rec, rel->schema);
    pinPage(mgt->rtf->bufferManager, pageHandle, pageNumber);
    memcpy(pageHandle->data + (slotNumber*mgt->rtf->sizeOfSlot), rString, strlen(rString));
    markDirty(mgt->rtf->bufferManager, pageHandle);
    unpinPage(mgt->rtf->bufferManager, pageHandle);
    forcePage(mgt->rtf->bufferManager, pageHandle);
    mgt->rec->id.tombS = false;
    mgt->rtf->numTuples+=1;
    char *temp;
    tableInfoToFile(rel->name,temp);
    free(pageHandle);
    free(rString);
}

void pgHandleOpn(int pageNumber,int slotNumber,RM_TableData *rel){


    pgOpn(pageNumber,slotNumber,rel);
}


RC writeSchemaToTable(SM_FileHandle fHandle){
    RC flag;
    int i;
    char *TableInfoString = tableToStr(mgt->rtf);

    if ((flag=writeBlock(0, &fHandle, TableInfoString)) == RC_OK){

        char *schemaToString = serializeSchema(mgt->sch);
        if ((flag=writeBlock(1, &fHandle, schemaToString)) == RC_OK){
            if ((flag=closePageFile(&fHandle)) == RC_OK){
                i=1;
            }else{
                return flag;
            }

        }else{
            return flag;

        }
    }else{
        return flag;
    }

}


/*********************************************************************************
 * Function Name - createTable
 *
 * Description:
 * 			This function creates a new Table. 	
 *
 * Parameters:
 *
 * 			char *name 
 *			Schema *schema
 * 			 
 * Return:
 * 			RC returned code
 *
 *
 *********************************************************************************/
RC createTable (char *name, Schema *schema){

    if(createPageFile(name)==RC_OK){
        RC flag;
        int k=0,sizeofSchema=0,sizeOfSlot=15,dtLength=0,fileSize=0,maximumSlots=0;
        for(k = 0;k<mgt->sch->numAttr;k++)
        {
            sizeofSchema=sizeofSchema+ strlen(schema->attrNames[k]);

        }

        sizeofSchema+=4+4+4*(mgt->sch->numAttr)+4*(mgt->sch->numAttr)+4*(mgt->sch->keySize);
        fileSize=(int)(ceil((float)sizeofSchema/PAGE_SIZE));
        k = 0;
        while(k<mgt->sch->numAttr)
        {
            if(mgt->sch->dataTypes[k]==DT_STRING){
                dtLength=mgt->sch->typeLength[k];
            }else if(mgt->sch->dataTypes[k]==DT_BOOL){
                dtLength=5;
            }else if(mgt->sch->dataTypes[k]==DT_INT){
                dtLength=5;
            }else if(mgt->sch->dataTypes[k]==DT_FLOAT){
                dtLength=10;
            }
            sizeOfSlot=sizeOfSlot+(dtLength + strlen(mgt->sch->attrNames[k]) + 2);
        }

        maximumSlots=(int)(floor((double)(PAGE_SIZE/sizeOfSlot)));

        SM_FileHandle fHandle;
        flag=openPageFile(name, &fHandle);
        if(flag!=RC_OK){
            return flag;
        }else{
            ensureCapacity((fileSize + 1), &fHandle);
            mgt->rtf=(RM_Mgmt *) malloc(sizeof(RM_Mgmt));
            mgt->rtf->maxPgSlt=maximumSlots;
            mgt->rtf->numTuples=0;
            mgt->rtf->sizeofSchema=sizeofSchema;
            mgt->rtf->sizeOfSlot=sizeOfSlot;
            mgt->rtf->recordStart=fileSize+1;
            mgt->rtf->recordEnd=fileSize+1;
            mgt->rtf->dataStoreHead = NULL;
            mgt->rtf->dataStoreInfoListSize=0;
            writeSchemaToTable(fHandle);


        }
        printf("asdfgh");
        return RC_OK;

    }
}


recordTableInfo *initBufMgrForRec(char *name,struct BM_BufferPool *bManager,struct BM_PageHandle *pageHandle){

    initBufferPool(bManager, name, 3, RS_FIFO, NULL);
    pinPage(bManager, pageHandle, 0);
    mgt->rtf = StrToRecTable(pageHandle->data);
    if(mgt->rtf->sizeofSchema < PAGE_SIZE){
        pinPage(bManager, pageHandle, 1);
    }
    return mgt->rtf;
}


/*********************************************************************************
 * Function Name - initRecordManager
 *
 * Description:
 * 			Initializes the Record Manager and also the BufferPool
 *
 * Parameters:
 *
 * 			void *mgmtData
 * 			 
 * Return:
 * 			RC returned code
 *
 *
 *********************************************************************************/
extern RC initRecordManager (void *mgmtData)
{
	printf("adf");
	mgt = (mgt == NULL ? MAKE_MGMT_INFO():mgt);
    return RC_OK;
}


/*********************************************************************************
 * Function Name - openTable
 *
 * Description:
 * 			This function opens a table. 
 *
 * Parameters:
 *
 * 			RM_TableData *rel  
 *			char *name
 * 			 
 * Return:
 * 			RC returned code
 *
 *
 *********************************************************************************/
RC openTable (RM_TableData *rel, char *name){

    if(access(name,0)!=-1){
        BM_BufferPool *bManager = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
        BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
        mgt->rtf = initBufMgrForRec(name,bManager,pageHandle);
        char temp;
        int count=mgt->rtf->numTuples;
        rel->schema = schemaDeslrzd(pageHandle->data,temp,count);
        rel->name = name;
        mgt->rtf->bufferManager = bManager;
        rel->mgmtData = mgt->rtf;
        free(pageHandle);
        return RC_OK;

    }else{
    	printf("TABLE_DOES_NOT_EXIST");
        return TABLE_DOES_NOT_EXIST;
    }
}


/*********************************************************************************
 * Function Name - closeTable
 *
 * Description:
 * 			This function closes the table.
 *
 * Parameters:
 *
 * 			RM_TableData *rel  
 * 			 
 * Return:
 * 			RC returned code
 *
 *********************************************************************************/
RC closeTable (RM_TableData *rel){
    shutdownBufferPool(((recordTableInfo *)rel->mgmtData)->bufferManager);
    free(mgt->td->schema->dataTypes);
    free(mgt->td->schema->keyAttrs);
    free(mgt->td->schema->attrNames);
    free(mgt->td->schema->typeLength);
    free(mgt->td->mgmtData);
    free(mgt->td->schema);
    return RC_OK;
}


/*********************************************************************************
 * Function Name - deleteTable
 *
 * Description:
 * 			This function deletes the table by calling destroyPageFile from the Buffer Manager	
 *
 * Parameters:
 *
 * 			char *name  
 * 			 
 * Return:
 * 			RC returned code
 *
 *********************************************************************************/
RC deleteTable (char *name){
    RC result;
    
    if(access(name, 0) != -1) {
        result = ((remove(name) == 0)?RC_OK:TABLE_DOES_NOT_EXIST);

    }
    else{

        result = TABLE_DOES_NOT_EXIST;
    }
    return result;
}


/*********************************************************************************
 * Function Name - shutdownRecordManager
 *
 * Description:
 * 			Shuts down the Record Manager 
 *
 * Return:
 * 			RC returned code
 *
 *
 *********************************************************************************/
RC shutdownRecordManager (){
    return RC_OK;
}


/*********************************************************************************
 * Function Name - getRecordSize
 *
 * Description:
 * 			This function is used to get the size of the record
 *
 * Parameters:
 *
 * 			Schema *schema
 * 			 
 * Return:
 * 			Integer (size of the record)
 *
 *********************************************************************************/
int getRecordSize (Schema *schema){
    int recordSize=0,counter=0;
    while(counter<schema->numAttr){

        if(schema->dataTypes[counter]==DT_STRING){
            recordSize+=schema->typeLength[counter];

        }else if(schema->dataTypes[counter]==DT_BOOL){
            recordSize+=sizeof(bool);

        }else if(schema->dataTypes[counter]==DT_INT){
            recordSize+=sizeof(int);

        }else if(schema->dataTypes[counter]==DT_FLOAT){
            recordSize+=sizeof(float);
        }
        counter++;
    }
    return recordSize;
}


/*********************************************************************************
 * Function Name - createSchema
 *
 * Description:
 * 			This function is used to create the schema for the table
 *
 * Parameters:
 *
 * 			int numAttr
 *			char **attrNames
 *			DataType *dataTypes
 *			int *typeLength
 *			int keySize
 * 			 
 * Return:
 * 			Schema
 *
 *********************************************************************************/
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    schema->attrNames = attrNames;
    schema->numAttr = numAttr;
    schema->keySize = keySize;
    schema->dataTypes = dataTypes;
    schema->attrNames = attrNames;
    schema->typeLength = typeLength;
    schema->keyAttrs = keys;
    return schema;
}


/*********************************************************************************
 * Function Name - freeSchema
 *
 * Description:
 * 			This function is used to free the memory of the variables used
 *
 * Parameters:
 *
 * 			Schema *schema
 * 			 
 * Return:
 * 			RC Returned Code
 *
 *********************************************************************************/
RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;
}


/*********************************************************************************
 * Function Name - insertRecord
 *
 * Description:
 * 			This function is responsible for inserting the records into the table
 *
 * Parameters:
 *
 * 			RM_TableData *rel 
 *			Record *record 
 * 			 
 * Return:
 * 			RC returned code
 *
 *********************************************************************************/
RC insertRecord (RM_TableData *rel, Record *record){


    int pageNumber, slotNumber,temp=0;
    //get the record manager related info.
    //recordTableInfo *recordTable=getRecMgrTableInfo(rel);
    mgt->rtf = getRecMgrTableInfo(rel);
    if (mgt->rtf->dataStoreHead == NULL){
        //if no tombstone record found
        //then insert at the end of last recod at page
        pageNumber = mgt->rtf->recordEnd;
        slotNumber = mgt->rtf->numTuples - ((pageNumber - mgt->rtf->recordStart)*mgt->rtf->maxPgSlt);

        if (slotNumber == mgt->rtf->maxPgSlt){
            slotNumber = 0;
            pageNumber+=1;
        }else{

        }

        //saving pointer to last inserted record
        mgt->rtf->recordEnd = pageNumber;
    }
    else{
        //if any recod with tombstone then insert new record at that place
        temp=1;
        int slotNumber,i;
        for(i = 0;i<1;i++){
        slotNumber = mgt->rtf->dataStoreHead->id.slot;
        i++;
        }
        //slotNumber = setSlotNumber(recordTable);
        int pageNumber;
    	pageNumber = mgt->rtf->dataStoreHead->id.page;
    	//pointing to next tombstone record
    	mgt->rtf->dataStoreHead = mgt->rtf->dataStoreHead->next;
        //pageNumber = setPageNumber(recordTable);
    }

    updateRecordInfo(pageNumber,slotNumber);
    pgHandleOpn(pageNumber,slotNumber,rel);
    return RC_OK;
}

void updateRecordInfo(int pageNumber,int slotNumber){

    mgt->rec->id.page = pageNumber;
    mgt->rec->id.slot = slotNumber;
}




RC createRecord (Record **record, Schema *schema){
    *record = (Record *)  malloc(sizeof(Record));
    (*record)->data = (char *)malloc((getRecordSize(schema)));
    //mgt->rec = (Record *) malloc(sizeof(Record));
    //mgt->rec->data = (char *)malloc((getRecordSize(mgt->sch)));
    return RC_OK;
}


int setPositionalDifference(Schema *schema, int attrNum){
    int positionalDifference;
    attrOffset(schema, attrNum, &positionalDifference);
    return positionalDifference;
}


/*********************************************************************************
 * Function Name - getAttr
 *
 * Description:
 * 			This function is used to get the value of attribute in record
 *
 * Parameters:
 *
 * 			Record *record
 *			Schema *schema
 *			int attrNum 
 * 			Value **value
 * 			 
 * Return:
 * 			RC Returned Code
 *
 *********************************************************************************/
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int length;
    int positionalDifference;
    //allocating memory space to value
    *value = (Value *)  malloc(sizeof(Value));
    (*value)->dt = schema->dataTypes[attrNum];
    char *attribute, *sValue;
    //getting attribute value position for each attributes
    positionalDifference=setPositionalDifference(schema,attrNum);
    attribute = (record->data) + positionalDifference;
    if(schema->dataTypes[attrNum]==DT_STRING){
        length = schema->typeLength[attrNum];
        sValue = (char *) malloc(length + 1);
    }
    if(schema->dataTypes[attrNum]==DT_INT){//if attribute is Int
        memcpy(&((*value)->v.intV),attribute, sizeof(int));
    }else if(schema->dataTypes[attrNum]==DT_BOOL){//if attribute is Boolean
        memcpy(&((*value)->v.boolV),attribute, sizeof(bool));
    }else if(schema->dataTypes[attrNum]==DT_FLOAT){//if attribute is Float
        memcpy(&((*value)->v.floatV),attribute, sizeof(float));
    }else if(schema->dataTypes[attrNum]==DT_STRING){//if attribute is String
        //setStringAttribute(attribute,sValue,length,value);
        strncpy(sValue, attribute, length);
    	sValue[length] = '\0';
    	(*value)->v.stringV = sValue;
    	}
    return RC_OK;
}


/*********************************************************************************
 * Function Name - setAttr
 *
 * Description:
 * 			This function is used to set the value of attribute of the record
 *
 * Parameters:
 *
 * 			Record *record
 *			Schema *schema
 *			int attrNum 
 * 			Value *value
 * 			 
 * Return:
 * 			RC Returned Code
 *
 *********************************************************************************/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int length;
    int positionalDifference;
    char *attribute, *sValue;
    //mgt->sch = (RM_Mgmt *)  malloc(sizeof(RM_Mgmt));
    //mgt->sch = schema;
    //memcpy(mgt->sch, schema, sizeof(schema));
    //getting attribute value position for each attributes
    positionalDifference=setPositionalDifference(schema,attrNum);
    attribute = (record->data) + positionalDifference;
    if(schema->dataTypes[attrNum]==DT_STRING){
        length = schema->typeLength[attrNum];
        sValue = (char *) malloc(length);
        sValue = value->v.stringV;
    }
    if(schema->dataTypes[attrNum]==DT_INT){//if attribute is Int
        memcpy(attribute,&(value->v.intV), sizeof(int));
    }else if(schema->dataTypes[attrNum]==DT_BOOL){//if attribute is Boolean
        memcpy(attribute,&((value->v.boolV)), sizeof(bool));
    }else if(schema->dataTypes[attrNum]==DT_FLOAT){//if attribute is Float
        memcpy(attribute,&((value->v.floatV)), sizeof(float));
    }else if(schema->dataTypes[attrNum]==DT_STRING){//if attribute is String
        memcpy(attribute,(sValue), length);
    }
    return RC_OK;
}



recordSearch *initSearchRecord(){
    mgt->srchRecord = (RM_Mgmt *) malloc(sizeof(RM_Mgmt));
    //recordSearch *recordSearch = (recordSearch *) malloc(sizeof(recordSearch));
    return mgt->srchRecord;
}


void initialize_Scanner(Expr *cond){
    mgt->srchRecord->slotsPerRelation = ((recordTableInfo *)mgt->td->mgmtData)->sizeOfSlot;
    mgt->srchRecord->pagesPerRelation = ((recordTableInfo *)mgt->td->mgmtData)->recordEnd;
    mgt->srchRecord->currRecPgNo = ((recordTableInfo *)mgt->td->mgmtData)->recordStart;
    mgt->srchRecord->currRecSlotNo = 0;
    mgt->srchRecord->condition = cond;
}


//seraches record in page
void findRecordInScheme(RM_ScanHandle *scan, Expr *cond) {
    //initializing RM_ScanHandle data structure
    scan->rel = mgt->td;
    //initializing recordSearch to store information about record to
    //searched and to evaluate a condition
    mgt->srchRecord = initSearchRecord();
    initialize_Scanner(cond);
    // recordSearch to scan->mgmtData
    scan->mgmtData = (void *) mgt->srchRecord;
}

//start scan
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    findRecordInScheme(scan, cond);
    return RC_OK;
}

//returns the scan pointer
recordSearch *getSearchRecordPointer(){
    mgt->srchRecord=mgt->sh->mgmtData;
    return mgt->srchRecord;
}

//assigns value to record slot and page
void populateScanData(Record *record){
    record->id.slot = mgt->srchRecord->currRecSlotNo;
    record->id.page = mgt->srchRecord->currRecPgNo;
}


void updateRecordSearch(){
    mgt->srchRecord->currRecPgNo = mgt->srchRecord->currRecPgNo+1;
    mgt->srchRecord->currRecSlotNo = 0;
}

void Evaluate(Record *record,Value **value){
    evalExpr(record, mgt->sh->rel->schema, mgt->srchRecord->condition, &(*value));
}

RC next (RM_ScanHandle *scan, Record *record){
    RC flag;
    Value *value;
    mgt->srchRecord=getSearchRecordPointer();
    populateScanData(record);

    //retrieve record with respect to page number and record id
    flag = getRecord(scan->rel, record->id, record);
    if(getRecord(scan->rel, record->id, record) != RC_RM_NO_MORE_TUPLES){
        //if record is deleted (means marked as tombstone)
        if(record->id.tombS){
            if (mgt->srchRecord->currRecSlotNo == mgt->srchRecord->slotsPerRelation - 1){
                updateRecordSearch();
                scan->mgmtData = mgt->srchRecord;
                return next(scan, record);
            }
            else{
                //if not increase the sacn pointer to next record
                mgt->srchRecord->currRecSlotNo = mgt->srchRecord->currRecSlotNo + 1;
                scan->mgmtData = mgt->srchRecord;
                return next(scan, record);
            }

        }
        else{

            Evaluate(record,&value);

            if (mgt->srchRecord->currRecSlotNo != mgt->srchRecord->slotsPerRelation - 1){
                mgt->srchRecord->currRecSlotNo = mgt->srchRecord->currRecSlotNo + 1;
                scan->mgmtData = mgt->srchRecord;
            }
            else{
                updateRecordSearch();
                scan->mgmtData = mgt->srchRecord;
            }

            if(value->v.boolV!=1){
                return next(scan, record);
            }
            else{
                return RC_OK;
            }
        }
    }else{
       
        return RC_RM_NO_MORE_TUPLES;
    }


}


/*********************************************************************************
 * Function Name - closeScan
 *
 * Description:
 * 			This function is used to end the scanning process through the table
 *
 * Parameters:
 *
 *			RM_ScanHandle *scan
 * 			 
 * Return:
 * 			RC returned code
 *
 *********************************************************************************/
RC closeScan (RM_ScanHandle *scan){
    return RC_OK;
}

//as record is deleted add tombstone mark to that slot
void updateTombStone(RID id,RM_TableData *rel){
    char *temp;
    mgt->dsNode->id.page = id.page;
    mgt->dsNode->id.slot = id.slot;
    mgt->dsNode->id.tombS = TRUE;
    (mgt->rtf->numTuples)-=1;
    tableInfoToFile(rel->name, temp);
}


//delete the record based on record id
RC deleteRecord (RM_TableData *rel, RID id){
    mgt->rtf = getRecMgrTableInfo(rel);
    mgt->dsNode = mgt->rtf->dataStoreHead;
   
    if(mgt->rtf->numTuples<0){
       
        return RC_WRITE_FAILED;
    }
    if(mgt->rtf->numTuples>0){
        if(mgt->rtf->dataStoreHead != NULL){
	    while(mgt->dsNode->next != NULL)
            //for (i=0;mgt->dsNode->next != NULL;i++)
	    {
                mgt->dsNode = mgt->dsNode->next;
            }
            mgt->dsNode->next = getDataStoreNode();
            mgt->dsNode = mgt->dsNode->next;
            updateTombStone(id,rel);
        }
        else{
            mgt->rtf->dataStoreHead = getDataStoreNode();
            mgt->rtf->dataStoreHead->next = NULL;
            mgt->dsNode = mgt->rtf->dataStoreHead;
            updateTombStone(id,rel);
        }

    }

    return RC_OK;
}
/*
void updatePageAndSlot(int *pageNumber,int *slotNumber,Record *record){
    *pageNumber = record->id.page;
    *slotNumber = record->id.slot;
}
*/
void updateOperations(RM_TableData *rel, BM_PageHandle *pageHandle){
    markDirty(mgt->rtf->bufferManager, pageHandle);
    unpinPage(mgt->rtf->bufferManager, pageHandle);
    forcePage(mgt->rtf->bufferManager, pageHandle);
    char *temp;
    tableInfoToFile(rel->name, temp);
}


//update the record data
RC updateRecord (RM_TableData *rel, Record *record){
    //BM_PageHandle *pageHandle = getBufferPageHandle();
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    mgt->rtf = getRecMgrTableInfo(rel);
    int pageNumber, slotNumber;
    //updatePageAndSlot(&pageNumber,&slotNumber,record);
    pageNumber = record->id.page;
    slotNumber = record->id.slot;
    char *stringRecord = serializeRecord(record, rel->schema);
    pinPage(mgt->rtf->bufferManager, pageHandle, pageNumber);
    memcpy((slotNumber*(mgt->rtf->sizeOfSlot))+pageHandle->data, stringRecord, strlen(stringRecord));
    updateOperations(rel,pageHandle);
    return RC_OK;

}
/*
Record *getRecordInitilaized(){
    Record *record = (Record *) malloc(sizeof(Record));
    return record;
}
*/
char *getRecordDataInitialized(){
    char *data = (char *)malloc(sizeof(char) * mgt->rtf->sizeOfSlot);
    return data;
}
/*
char *getToken(char *recData){
    char *t;
    t = strtok(recData,"-");
    t = strtok (NULL,"]");
    t = strtok (NULL,"(");
    return t;
}
*/
void setAttributeValues(Record *record, Schema *schema, int loop,Value *value,char *s2){
    setAttr (record, schema, loop, value);
}

void dBasedOnDataType(char *s1, int loop){
    //Value *value;
    char *s2;
    if(mgt->sch->dataTypes[loop]==DT_INT)
    {
        int intVal = strtol(s1, &s2, 10);
        MAKE_VALUE(mgt->value, DT_INT, intVal);
        setAttributeValues(mgt->rec, mgt->sch, loop, mgt->value,s2);
    }
    else if(mgt->sch->dataTypes[loop]==DT_STRING)
    {
        MAKE_STRING_VALUE(mgt->value, s1);
        setAttributeValues(mgt->rec, mgt->sch, loop, mgt->value,s2);
    }
    else if(mgt->sch->dataTypes[loop]==DT_FLOAT)
    {
        float floatVal = strtof(s1, NULL);
        MAKE_VALUE(mgt->value, DT_FLOAT, floatVal);
        setAttributeValues(mgt->rec, mgt->sch, loop, mgt->value,s2);
    }
    else if(mgt->sch->dataTypes[loop]==DT_BOOL)
    {
        bool boolVal;
	boolVal = ((s1[0] == 't')?TRUE:FALSE);

        MAKE_VALUE(mgt->value, DT_BOOL, boolVal);
        setAttributeValues(mgt->rec, mgt->sch, loop, mgt->value,s2);
    }
    freeVal(mgt->value);
}

//deSerializing of Record
Record *dRecord(RM_TableData *rel,char *stringRecord){
    char recData[strlen(stringRecord)];
    strcpy(recData, stringRecord);
    mgt->sch = rel->schema;
    int loop=0,temp=0;
    mgt->rtf = getRecMgrTableInfo(rel);
    mgt->rec = (Record *) malloc(sizeof(Record));
    //Record *record = getRecordInitilaized();
    
    mgt->rec->data = getRecordDataInitialized();
    char *s1;
    char *t;
    t = strtok(recData,"-");
    t = strtok (NULL,"]");
    t = strtok (NULL,"(");
    s1=t;
    free(stringRecord);
    int i;
    for(i = 0;loop<mgt->sch->numAttr;i++)

    {
        s1 = strtok (NULL,":");
	s1 = ((loop == (mgt->sch->numAttr - 1))?strtok (NULL,")"):strtok (NULL,","));

        dBasedOnDataType(s1,loop);
        loop+=1;
        temp=temp+1;
	
    }
    return mgt->rec;
}


bool checkTombStone(RM_TableData *rel,RID id,int *pageNumber,int *slotNumber, int *tcount){
    mgt->rtf=getRecMgrTableInfo(rel);
    mgt->dsNode = (mgt->rtf)->dataStoreHead;

    bool tFlag=false;
    int i;
    for(i=0;i<mgt->rtf->dataStoreLength && mgt->dsNode!=NULL;i++)
    //while(i<(recordTable)->dataStoreLength && tombNode!=NULL)
    {

        if (mgt->dsNode->id.page == *pageNumber && mgt->dsNode->id.slot == *slotNumber){
            tFlag = true;
            mgt->rec->id.tombS = true;
            //break;
        }

        mgt->dsNode = mgt->dsNode->next;
        (*tcount)+=1;
      
    }
    return tFlag;
}


void updateRecordInformation(int *pageNumber,int *slotNumber){
    //populating record ID and slot to
    //record where new record inserted

    mgt->rec->id.page = *pageNumber;
    mgt->rec->id.slot = *slotNumber;
}

void getRecordPageOperation(RM_TableData *rel,Record *record,BM_PageHandle *pageHandle,int pageNumber,int slotNumber){
    char *stringRecord = (char *) malloc(sizeof(char) * mgt->rtf->sizeOfSlot);
    pinPage(mgt->rtf->bufferManager, pageHandle, pageNumber);
    memcpy(stringRecord, pageHandle->data + ((slotNumber)*mgt->rtf->sizeOfSlot), sizeof(char)*mgt->rtf->sizeOfSlot);
    unpinPage(mgt->rtf->bufferManager, pageHandle);
    Record *tempR = dRecord(rel,stringRecord);
    record->data = tempR->data;
    free(tempR);
    free(pageHandle);
}

//get the record based on record ID
RC getRecord (RM_TableData *rel, RID id, Record *record){

    //BM_PageHandle *pageHandle=getBufferPageHandle();
    BM_PageHandle *pageHandle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    mgt->rtf=getRecMgrTableInfo(rel);
    int pageNumber, slotNumber;
    pageNumber = id.page;
    slotNumber = id.slot;
    //updateIDBased(id,&pageNumber,&slotNumber);
    updateRecordInformation(&pageNumber,&slotNumber);

    mgt->rec->id.tombS = false;
    int tcount=0;

    bool tFlag=checkTombStone(rel,id,&pageNumber,&slotNumber,&tcount);

    if(pageHandle!=NULL){
        if (!tFlag){
            int tupleNumber = (pageNumber - mgt->rtf->recordStart)*(mgt->rtf->maxPgSlt) + slotNumber + 1 - tcount;
            if (mgt->rtf->numTuples>=tupleNumber){
                getRecordPageOperation(rel,mgt->rec, pageHandle,pageNumber,slotNumber);
            }else{
                free(pageHandle);
                return RC_RM_NO_MORE_TUPLES;
            }
        }
    }else{
        free(pageHandle);
    }
    return RC_OK;
}
