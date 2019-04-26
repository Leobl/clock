/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/* 
 Write all the modified pages to disk and free the memory
 allocated for buf description and the hashtable.
*/
BufMgr::~BufMgr() {
  for(FrameId i =0; i<numBufs;i++){
    if(bufDescTable[i].dirty == true)
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  free(bufDescTable);
  delete[] bufPool;
  free(hashTable);

  /*
    // 1. Flush all dirty pages to disk
    for ( std::uint32_t i = 0 ; i < numBufs; ++i ) {
        if (bufDescTable[i].dirty) {
            bufDescTable[i].file->writePage(bufPool[i]);
        }
    }
    // 2. deallocate buffer pool
    delete hashTable; // ?? not mentioned in the pdf
    delete [] bufPool;
    // 3. deallocate BufDesc table
    delete [] bufDescTable;
    */
}

/* 
 Increment the clockhand within the circular buffer pool .
*/
void BufMgr::advanceClock() {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
    if (++clockHand >= numBufs){
        clockHand=0;
    }
}

/*
 This function allocates a new frame in the buffer pool 
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
void BufMgr::allocBuf(FrameId & frame) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */

    std::cout << "allocbuf" << std::endl;
    int counter = 0;
    int initial = clockHand;
    while (bufDescTable[clockHand].valid)
    {
        advanceClock();
        if(clockHand == initial) counter=0;
        if (bufDescTable[clockHand].refbit)
        {
            bufDescTable[clockHand].refbit = false;
        }else{
            if (bufDescTable[clockHand].pinCnt <1 )
            {
                if (bufDescTable[clockHand].dirty)
                {
                    /* code  scrivi in memomoria */
                    (bufDescTable[clockHand].file)->writePage(bufPool[clockHand]);
                    bufDescTable[clockHand].dirty = false;
                    frame= clockHand;
                    return;
                }else{
                    /* code set */
                    frame= clockHand;
                    return;
                }
            }else{
                if (++counter == numBufs)
                {
                    throw BufferExceededException();
                }
            }
        }
    }
    frame= clockHand;
    //std::cout << "end fuck" << std::endl;
   
}

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page. 
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */

    std::cout << "readPage" << std::endl;

    // printSelf();
    try{
        FrameId frameNumber;
        hashTable->lookup(file, pageNo, frameNumber);
        std::cout << "penis" << std::endl;
        bufDescTable[frameNumber].refbit= true;
        bufDescTable[frameNumber].pinCnt++;
        page = &(bufPool[frameNumber]);

    }catch (HashNotFoundException& e){
        FrameId frameNu;
        try{
            allocBuf(frameNu);
            //std::cout << "sono arrivato" << std::endl;
            Page temp = file->readPage(pageNo);
            //std::cout << "sono arrivato1" << std::endl;
            bufPool[frameNu] = temp;
            page = &temp;
            //std::cout << "sono arrivato2" << std::endl;
            hashTable->insert(file, pageNo, frameNu);
            //std::cout << "sono arrivato3" << std::endl;
            bufDescTable[frameNu].Set(file, pageNo);
            //std::cout << "sono arrivato4" << std::endl;
        }
        catch(...){
            //std::cout << "errore" << std::endl;
        }
    }

    /*
    FrameId frameNo;
    if ( hashTable->lookup(file, pageNo, frameNo) ) { // found
        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt++;
    } else { // not found
        allocBuf(frameNo);
        bufPool[frameNo] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
    }
    page = &bufPool[frameNo];
    */

}

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
    //std::cout << "unPinPage" << std::endl;
    try{
        FrameId frameNumb;
        hashTable->lookup(file, pageNo, frameNumb);
        if(bufDescTable[frameNumb].pinCnt<=0){
            throw PageNotPinnedException(file->filename(), pageNo, frameNumb);
        }else
        {
            bufDescTable[frameNumb].pinCnt--;
            if (dirty)
            {
                bufDescTable[frameNumb].dirty=true;
            }
        }

    }catch(HashNotFoundException& e){
        //std::cout << "exception in unPinPage" << std::endl;
    }

    /*
    FrameId frameNo;
    if ( hashTable->lookup(file, pageNo, frameNo)) { // found
        if ( dirty ) bufDescTable[frameNo].dirty = true;
        if ( bufDescTable[frameNo].pinCnt > 0 ) {
            bufDescTable[frameNo].pinCnt--;
        } else {
            throw PageNotPinnedException(file->filename(), pageNo, frameNo);
        }
    }
    */
}

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
void BufMgr::flushFile(const File* file) {

	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
    //std::cout << "flushFile" << std::endl;
    for (int i = 0; i < numBufs; ++i)
    {
        if (bufDescTable[i].file == file){
            if (bufDescTable[i].pinCnt>0)
            {
                throw PagePinnedException(file->filename(),bufDescTable[i].pageNo, bufDescTable[i].frameNo );
            }else if(!bufDescTable[i].valid){
                throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, false, bufDescTable[i].refbit);
            }else{
                if (bufDescTable[i].dirty)
                {
                    bufDescTable[i].file->writePage(bufPool[i]);
                    bufDescTable[i].dirty = false;
                }
                hashTable->remove(file, bufDescTable[i].pageNo);
                bufDescTable[i].Clear();
            }
        }
    }

    /*
    BufDesc* tmpbuf;
    for (std::uint32_t i = 0 ; i < numBufs ; ++i ) {
      tmpbuf = &bufDescTable[i];
      if ( tmpbuf->file == file ) { //Is this the way to check if belong to same file?
      if ( tmpbuf->pinCnt > 0 )  {
        throw PagePinnedException(file->filename(), tmpbuf->pageNo, tmpbuf->frameNo);
      }
      if ( ! tmpbuf->valid ) {
        throw BadBufferException(tmpbuf->frameNo, tmpbuf->dirty, tmpbuf->valid, tmpbuf->refbit);
      }
      if ( tmpbuf->dirty )
        tmpbuf->file->writePage(bufPool[i]);
      hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
      tmpbuf->Clear();

      }
    }
    */
}

/*
 This function allocates a new page and reads it into the buffer pool.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {
	
	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
    //std::cout << "allocPage" << std::endl;
    try{
        Page temp = file->allocatePage();
        PageId pageNumb=temp.page_number();
        FrameId frameinno;
        allocBuf(frameinno);

        hashTable->insert(file, pageNumb, frameinno);
        bufDescTable[frameinno].Set(file, pageNumb);
        bufPool[frameinno] = temp;
        page= &(bufPool[frameinno]);
        pageNo = pageNumb;

    }catch(...){

    }

    /*
    FrameId frameNo;
    allocBuf(frameNo);
    bufPool[frameNo] = file->allocatePage();
    pageNo = bufPool[frameNo].page_number();
    // fprintf(stderr, "info: %s +%d %s\n", __FILE__, __LINE__, __func__);

    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];
     */
}

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
void BufMgr::disposePage(File* file, const PageId PageNo) {
  
	/* ============== */	
	/* YOUR CODE HERE */
	/* ============== */
    std::cout << "disposePage" << std::endl;
    try{
        FrameId framenumberino;
        hashTable->lookup(file, PageNo, framenumberino);
        bufDescTable[framenumberino].valid=false;
        bufDescTable[framenumberino].Clear();
        hashTable->remove(file, PageNo);
        file->deletePage(PageNo);
    }catch(...){

    }

    /*
    FrameId frameNo;
    if ( hashTable->lookup(file, PageNo, frameNo) ) {
      BufDesc* tmpbuf = &bufDescTable[frameNo];
      hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
      tmpbuf->Clear();
    }
    file->deletePage(PageNo);
    */
	
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
