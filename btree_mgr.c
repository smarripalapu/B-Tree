
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "record_mgr.h"
#include "btree_mgr.h"



//Management Structure for BTree Representation
typedef struct BTree
{
	struct Value value;
	struct RID rid;
	int numOfKeys;
	BM_BufferPool *bm;
	BM_PageHandle *ph;
	int maxNumOfKeysPerNode;
	int nodeCounter;
}BTree;


BTree **bTreeCreate;


//Store the Number of Keys allocated in B+Tree
int numOfKeys;
int scanNextEntry;



/*
 * Function: initIndexManager
 * ---------------------------
 * This is function is used to Initialize Index Manager.
 *
 * mgmtData :
 *
 * returns : RC_OK if index manager initializing is successful.
 *
 */

RC initIndexManager (void *mgmtData)
{
	printf("Initializing index manager!");
	return RC_OK;
}

/*
 * Function: shutdownIndexManager
 * ---------------------------
 * This is function is used to shut down the index manager,
 * and free allocated memory to bTreeCreate during the process
 *
 *
 * returns : RC_OK if index manager initializing is successful.
 *
 */

RC shutdownIndexManager()
{
	int i =0;
	//for all keys inserted
	while(i<numOfKeys)
	{
		free(bTreeCreate[numOfKeys]);
		i+=1;
	}
	printf("Shutdown index manager done!");
	return RC_OK;
}

/*
 * Function: createBtree
 * ---------------------------
 * This function is used to Create a B+ Tree
 * and initialize attributes related to the tree.
 *
 * idxId:
 * keyType:
 * n:
 *
 * returns : RC_OK if index manager initializing is successful.
 *
 */

RC createBtree (char *idxId, DataType keyType, int n)
{
	SM_PageHandle ph = malloc(PAGE_SIZE*sizeof(char));
	SM_FileHandle fh;
	bTreeCreate = (BTree**)malloc(sizeof(BTree*));
	BTreeHandle *bt = (BTreeHandle*)malloc(sizeof(BTreeHandle*));

	RC createflag = createPageFile(idxId);
	if(createflag!= RC_OK){
		return createflag;
	}
	openPageFile(idxId,&fh);
	ensureCapacity(1,&fh);

	bt->idxId = idxId;
	bt->keyType = keyType;
	bt->mgmtData = n;

	*((int*)ph)=n;
	writeCurrentBlock(&fh,ph);
	closePageFile(&fh);
	numOfKeys = 0;
	scanNextEntry = 0;

}


/*
 * Function: openBtree
 * ---------------------------
 * This function is used to open the B-Tree alread created above,
 * it read the value from the page file regarding the "N"
 * and stores in the BTREE structure created attributes
 *
 * tree:
 * idxId:
 *
 * returns : RC_OK if open Btree is successful.
 *					RC_FILE_NOT_FOUND if openPagefile fails.
 *
 */

RC openBtree (BTreeHandle **tree, char *idxId)
{
	SM_FileHandle fh;
	RC openflag = openPageFile(idxId,&fh);
	if(openflag != RC_OK){
		return RC_FILE_NOT_FOUND;
	}
	*tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));
	BTree *treeInfo = (BTree*)malloc(sizeof(BTree));
	(*tree)->idxId = idxId;
	treeInfo->bm = MAKE_POOL();
	treeInfo->ph = MAKE_PAGE_HANDLE();

	initBufferPool(treeInfo->bm,idxId,6,RS_FIFO,NULL);
	pinPage(treeInfo->bm,treeInfo->ph,1);
	treeInfo->maxNumOfKeysPerNode = *((int*)treeInfo->ph->data);
	treeInfo->nodeCounter=0;

	BTreeHandle *temp = (BTreeHandle*)malloc(sizeof(BTreeHandle));
	temp->mgmtData = treeInfo;
	*tree = temp;

	unpinPage(treeInfo->bm,treeInfo->ph);
	closePageFile(&fh);

	return RC_OK;
}

/*
 * Function: closeBtree
 * ---------------------------
 * This function is used to close btree.
 *
 * tree:
 *
 * returns : RC_OK if freeing treehandle is successful.
 *
 */

RC closeBtree (BTreeHandle *tree)
{
	free(tree);
	printf("Closing Btree..");
	return RC_OK;
}

/*
 * Function: deleteBtree
 * ---------------------------
 * This function is used to destroy the page file created and free all memory created for btree.
 *
 * idxId:
 *
 * returns : RC_OK if destroyPageFile is successful.
 *
 */

RC deleteBtree (char *idxId)
{
	RC destroyFlag = destroyPageFile(idxId);
	if(destroyFlag != RC_OK){
		return destroyFlag;
	}
	numOfKeys = 0;
	scanNextEntry = 0;
	return RC_OK;
}

/*
 * Function: getNumNodes
 * ---------------------------
 * This function is used to get the number of nodes in b+tree.
 * This is caliculated by taking total number of keys inserted
 * MINUS same nodes will give us the final result
 *
 * tree:
 * result:
 *
 * returns : RC_OK if calculating number of nodes is successful.
 *
 */

RC getNumNodes (BTreeHandle *tree, int *result)
{

	int hnode = 1;
	int cnode, samePageEntry;

	while(hnode<numOfKeys)
	{
		cnode=hnode-1;
		while(cnode>=0)
		{
			if(bTreeCreate[hnode]->rid.page==bTreeCreate[cnode]->rid.page)
			{
				samePageEntry+=1;
			}
			cnode-=1;
		}
		hnode+=1;
	}
	*result = numOfKeys - samePageEntry;
	return RC_OK;
}

/*
 * Function: getNumEntries
 * ---------------------------
 * This function is used to return total number of keys in the B+Tree.
 *
 * tree:
 * result:
 *
 * returns : RC_OK if calculating number of keys is successful.
 *
 */

RC getNumEntries (BTreeHandle *tree, int *result)
{
	printf("Getting number of entries..");
	*result = numOfKeys;
	return RC_OK;
}

/*
 * Function: getKeyType
 * ---------------------------
 * This function is used get the type of Key in the tree which is inserted and stores it in the result.
 *
 * tree:
 * result:
 *
 * returns : RC_OK if getting keytype is successful.
 *
 */

RC getKeyType (BTreeHandle *tree, DataType *result)
{
	int i=0;

	while(i<numOfKeys)
	{
		result[i] = tree->keyType;
		i+=1;
	}
	return RC_OK;
}

/*
 * Function: findKey
 * ---------------------------
 * This function is used in searching for a key in the B+tree
 *
 * tree:
 * key:
 * result:
 *
 * returns : RC_OK if calculating number of nodes is successful.
 * 					RC_IM_KEY_NOT_FOUND if key is not found
 *					RC_RM_NO_PRINT_FOR_DATATYPE if datatypedoesnot match.
 */

RC findKey (BTreeHandle *tree, Value *key, RID *result)
{

	int i=0;
	int flagFound=0;

	while(i<numOfKeys)
	{

		if(key->dt==DT_INT)
		{
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.intV==key->v.intV)
				flagFound=1;
		}
		else if(key->dt==DT_FLOAT)
		{
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.floatV==key->v.floatV)
				flagFound=1;
		}
		else if(key->dt==DT_STRING)
		{
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.stringV==key->v.stringV)
				flagFound=1;

		}
		else{
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}

		if(flagFound)
		{
			result->page = bTreeCreate[i]->rid.page;
			result->slot = bTreeCreate[i]->rid.slot;
			return RC_OK;
		}
		i+=1;

	}
	if(i==numOfKeys)
	{
		return RC_IM_KEY_NOT_FOUND;
	}
}

/*
 * This function is used to insert Keys into the B+ Tree
 * We check if it is the first Node to be inserted,
 * if yes we insert it and increment the num of Keys
 * We also check whether the Key already exists,
 * if yes we return already exists
 * Mark the Node full when the node has "N" keys
 */
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	//allocate memory in the node for the Key to be inserted
	BTree *treeInfo = (BTree*)(tree->mgmtData);
	bTreeCreate[numOfKeys] = (BTree*)malloc(sizeof(BTree));

	//flag to check whether Key already exists
	int flagFound=0;

	//i.e. first key to be inserted
	if(numOfKeys==0)
	{
		pinPage(treeInfo->bm,treeInfo->ph,numOfKeys);
		markDirty(treeInfo->bm,treeInfo->ph);
		treeInfo->ph->data = "NotFull";

		switch(key->dt)
		{

		case DT_INT:
			bTreeCreate[numOfKeys]->value.dt = key->dt;
			bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
			break;

		case DT_FLOAT:
			bTreeCreate[numOfKeys]->value.dt = key->dt;
			bTreeCreate[numOfKeys]->value.v.floatV = key->v.floatV;
			break;

		case DT_STRING:
			bTreeCreate[numOfKeys]->value.dt = key->dt;
			bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
			break;
		default:
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}
		bTreeCreate[numOfKeys]->rid.page = rid.page;
		bTreeCreate[numOfKeys]->rid.slot = rid.slot;

		numOfKeys++;
		unpinPage(treeInfo->bm,treeInfo->ph);

		return RC_OK;

	}
	else
	{
		//find if the Key already Exists
		int i=0;
		for(i=0;i<numOfKeys;i++)
		{
			switch(key->dt)
			{

			case DT_INT:
				if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.intV==key->v.intV)
					flagFound=1;
				break;
			case DT_FLOAT:
				if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.floatV==key->v.floatV)
					flagFound=1;
				break;
			case DT_STRING:
				if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.stringV==key->v.stringV)
					flagFound=1;
				break;
			default:
				return RC_RM_NO_PRINT_FOR_DATATYPE;
			}
		}

		if(flagFound)	//key already exists
		{
			return RC_IM_KEY_ALREADY_EXISTS;
		}
		else	//insert the new Key
		{
			pinPage(treeInfo->bm,treeInfo->ph,treeInfo->nodeCounter);
			markDirty(treeInfo->bm,treeInfo->ph);

			if(!(strcmp(treeInfo->ph->data,"NodeFull")))	//if Node is Full //insert into other page
			{
				treeInfo->nodeCounter++;
				unpinPage(treeInfo->bm,treeInfo->ph);
				pinPage(treeInfo->bm,treeInfo->ph,treeInfo->nodeCounter);

				switch(key->dt)
				{
				case DT_INT:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
					break;

				case DT_FLOAT:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.floatV = key->v.floatV;
					break;

				case DT_STRING:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
					break;

				default:
					return RC_RM_NO_PRINT_FOR_DATATYPE;
				}

				//set the Rid & Value
				bTreeCreate[numOfKeys]->rid.page = rid.page;
				bTreeCreate[numOfKeys]->rid.slot = rid.slot;

				//increment the Keys and mark the new node as not full
				treeInfo->ph->data="NotFull";
				numOfKeys++;
				unpinPage(treeInfo->bm,treeInfo->ph);
			}
			else
			{
				switch(key->dt)
				{
				case DT_INT:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
					break;

				case DT_FLOAT:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.floatV = key->v.floatV;
					break;

				case DT_STRING:
					bTreeCreate[numOfKeys]->value.dt = key->dt;
					bTreeCreate[numOfKeys]->value.v.intV = key->v.intV;
					break;

				default:
					return RC_RM_NO_PRINT_FOR_DATATYPE;
				}

				//set the Rid & Value
				bTreeCreate[numOfKeys]->rid.page = rid.page;
				bTreeCreate[numOfKeys]->rid.slot = rid.slot;

				//increment the Keys and Make the Node Full as N values are inserted
				treeInfo->ph->data="NodeFull";
				numOfKeys++;
				unpinPage(treeInfo->bm,treeInfo->ph);

			}
			return RC_OK;
		}
	}
	return RC_OK;
}

/*
 * This function is used to Delete a Key from the Tree
 * Marks the node as Not full after deletion of Key from the Node
 */
RC deleteKey (BTreeHandle *tree, Value *key)
{
	//printf("Deleting B-Tree\n");

	BTree* treeInfo = (BTree*)(tree->mgmtData);

	int i=0;
	int flagFound=0;
	int deletedKeyIndex, nextKeyIndex;
	for(i = 0;i<numOfKeys;i++)
	{

		switch(key->dt)
		{
		case DT_INT:
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.intV==key->v.intV)
				flagFound=1;
			break;
		case DT_FLOAT:
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.floatV==key->v.floatV)
				flagFound=1;
			break;
		case DT_STRING:
			if(bTreeCreate[i]->value.dt==key->dt && bTreeCreate[i]->value.v.stringV==key->v.stringV)
				flagFound=1;
			break;
		default:
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}

		if(flagFound)	//if Key found
		{
			pinPage(treeInfo->bm,treeInfo->ph,(i/2));	//find the node page
			treeInfo->ph->data = "NotFull";				//mark it not full node

			markDirty(treeInfo->bm,treeInfo->ph);
			unpinPage(treeInfo->bm,treeInfo->ph);

			deletedKeyIndex=i;
			nextKeyIndex = deletedKeyIndex+1;
			//move the Keys and Nodes after deletion is done
			int k;
			for(k=deletedKeyIndex;k<numOfKeys && nextKeyIndex<numOfKeys;k++)
			{
				switch(bTreeCreate[nextKeyIndex]->value.dt)
				{
				case DT_INT:
					bTreeCreate[k]->value.dt = bTreeCreate[nextKeyIndex]->value.dt;
					bTreeCreate[k]->value.v.intV = bTreeCreate[nextKeyIndex]->value.v.intV;
					break;
				case DT_FLOAT:
					bTreeCreate[k]->value.dt = bTreeCreate[nextKeyIndex]->value.dt;
					bTreeCreate[k]->value.v.floatV = bTreeCreate[nextKeyIndex]->value.v.floatV;
					break;
				case DT_STRING:
					bTreeCreate[k]->value.dt = bTreeCreate[nextKeyIndex]->value.dt;
					strcpy(bTreeCreate[k]->value.v.stringV, bTreeCreate[nextKeyIndex]->value.v.stringV);
					break;
				}

				bTreeCreate[k]->rid.page = bTreeCreate[nextKeyIndex]->rid.page;
				bTreeCreate[k]->rid.slot = bTreeCreate[nextKeyIndex]->rid.slot;
				nextKeyIndex++;
			}
			numOfKeys--;
			free(bTreeCreate[k]);
			return RC_OK;
		}

		if(i == numOfKeys)
			return RC_IM_KEY_NOT_FOUND;
	}

	return RC_OK;
}

/*
 * Create a tree ready for Scan,
 * make all the Keys sorted for the Scanner to read them
 */
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	int i=0,j, indexSmallKey;
	Value tempValueSwap;
	RID tempRidSwap;

	switch(bTreeCreate[i]->value.dt)
	{
	case DT_INT:
		for(i=0;i<numOfKeys-1;i++)
		{
			indexSmallKey = i;

			//SORT
			for(j=i+1;j<numOfKeys;j++)
			{
				if(bTreeCreate[j]->value.v.intV < bTreeCreate[indexSmallKey]->value.v.intV)
				{
					indexSmallKey = j;
				}
			}
			tempValueSwap = bTreeCreate[indexSmallKey]->value;
			tempRidSwap = bTreeCreate[indexSmallKey]->rid;

			bTreeCreate[indexSmallKey]->value = bTreeCreate[i]->value;
			bTreeCreate[indexSmallKey]->rid = bTreeCreate[i]->rid;

			bTreeCreate[i]->value = tempValueSwap;
			bTreeCreate[i]->rid = tempRidSwap;
		}
		break;

	case DT_FLOAT:
		for(i=0;i<numOfKeys-1;i++)
		{
			indexSmallKey = i;

			for(j=i+1;j<numOfKeys;j++)
			{
				if(bTreeCreate[j]->value.v.floatV < bTreeCreate[indexSmallKey]->value.v.floatV)
				{
					indexSmallKey = j;
				}
			}
			tempValueSwap = bTreeCreate[indexSmallKey]->value;
			tempRidSwap = bTreeCreate[indexSmallKey]->rid;

			bTreeCreate[indexSmallKey]->value = bTreeCreate[i]->value;
			bTreeCreate[indexSmallKey]->rid = bTreeCreate[i]->rid;

			bTreeCreate[i]->value = tempValueSwap;
			bTreeCreate[i]->rid = tempRidSwap;
		}
		break;
	}
	return RC_OK;
}

/*
 * read the entry from the Tree until the No more entires are left in the tree and
 * store the page and slot info in result
 */
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	if(scanNextEntry < numOfKeys)
	{
		result->page = bTreeCreate[scanNextEntry]->rid.page;
		result->slot = bTreeCreate[scanNextEntry]->rid.slot;
		scanNextEntry++;
		return RC_OK;
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}

}

/*
 * Close the Scan for Tree
 */
RC closeTreeScan (BT_ScanHandle *handle)
{
	free(handle);
	return RC_OK;
}

// debug and test functions
/*
 * Print the B+ TREE Representation
 */
char *printTree (BTreeHandle *tree)
{
	int count = 1, tempCount =1;
	int i;
	char opString[500];
	char newopString[500];
	char finalResult[500]="";
	char TREE[500] = "";

	strcpy(opString,"1,");
	int compare = 2;

	for(i=0;i<numOfKeys;i++)
	{
		if(i%(compare) == 0 && i!=0)
		{
			sprintf(newopString,"%d",bTreeCreate[i]->value.v.intV);
			strcat(opString,newopString);
			strcat(opString,",");
			sprintf(newopString,"%d",(tempCount+1));
			strcat(opString,newopString);
			strcat(opString,",");
			tempCount++;
			count++;
		}

		if(i%(compare) == 0 && i!=0)
		{
			sprintf(newopString,"%d",count);
			strcpy(finalResult,newopString);
			strcat(finalResult,"\n");
		}
		sprintf(newopString,"%d",bTreeCreate[i]->rid.page);
		strcat(finalResult,newopString);
		strcat(finalResult,".");
		sprintf(newopString,"%d",bTreeCreate[i]->rid.slot);
		strcat(finalResult,newopString);
		strcat(finalResult,", ");
		sprintf(newopString,"%d",bTreeCreate[i]->value.v.intV);
		strcat(finalResult,newopString);
		strcat(finalResult,",");
		if(!(i%(compare) == 0 && i!=0))
		{
			strcat(TREE,finalResult);
		}

	}

	printf("%s\n",opString);
	printf("%s\n",TREE);
	return tree->idxId;
}
