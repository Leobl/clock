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
    }

/*
 Increment the clockhand within the circular buffer pool .
*/
    void BufMgr::advanceClock() {

        /* ============== */
        /* YOUR CODE HERE */
        /* ============== */

        if (++clockHand >= numBufs){ //increases clockHand
            clockHand=0; //clockHand set to initial value if a "circle" is completed
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

        std::cout << "allocBuf" << std::endl;
        int counter = 0;
        int initial = clockHand;
        //clock algorithm using while-loop
        while (bufDescTable[clockHand].valid) //checks if the page is valid
        {
            advanceClock();
            if(clockHand == initial)
                counter=0;
            if (bufDescTable[clockHand].refbit)
            {
                bufDescTable[clockHand].refbit = false; //clear refbit
            }else{
                if (bufDescTable[clockHand].pinCnt < 1 ) //the page is not being used
                {
                    if (bufDescTable[clockHand].dirty) //the page is modified
                    {
                        (bufDescTable[clockHand].file)->writePage(bufPool[clockHand]); //write page back to disk
                        bufDescTable[clockHand].dirty = false; //clear dirty
                        frame = clockHand;
                        return;
                    }else{
                        frame = clockHand;
                        return;
                    }
                }else{
                    if (++counter == numBufs) //increases counter
                    {
                        throw BufferExceededException(); //exception: all buffer frames are pinned
                    }
                }
            }
        }
        frame= clockHand;
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

        try{
            FrameId frameNumber;
            hashTable->lookup(file, pageNo, frameNumber); //checks if the page is in the hash table
            bufDescTable[frameNumber].refbit= true; //set refbit
            bufDescTable[frameNumber].pinCnt++; //increase pinCnt
            page = &(bufPool[frameNumber]); //returns a pointer to the page itself

        }catch (HashNotFoundException& e){ //the page does not exist
            FrameId frameNu;
            try{
                allocBuf(frameNu); //allocates a new frame for the page to be read
                Page temp = file->readPage(pageNo); //reads page from the file
                bufPool[frameNu] = temp;
                page = &temp;
                hashTable->insert(file, pageNo, frameNu); //insert page to the hash table
                bufDescTable[frameNu].Set(file, pageNo); //set frame table
            }
            catch(...){
            }
        }
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
        std::cout << "unPinPage" << std::endl;
        try{
            FrameId frameNumb;
            hashTable->lookup(file, pageNo, frameNumb);
            if(bufDescTable[frameNumb].pinCnt<=0){ //pinCnt is already 0
                throw PageNotPinnedException(file->filename(), pageNo, frameNumb);
            }else
            {
                bufDescTable[frameNumb].pinCnt--; //decrease pinCnt
                if (dirty) //page modified
                {
                    bufDescTable[frameNumb].dirty=true; //set dirty
                }
            }

        }catch(HashNotFoundException& e){ //exception caused by lookup() function
        }
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
        std::cout << "flushFile" << std::endl;
        for (int i = 0; i < numBufs; ++i)
        {
            if (bufDescTable[i].file == file){
                if (bufDescTable[i].pinCnt>0) //being referenced by other services
                {
                    throw PagePinnedException(file->filename(),bufDescTable[i].pageNo, bufDescTable[i].frameNo );
                }else if(!bufDescTable[i].valid){ //not valid
                    throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, false, bufDescTable[i].refbit);
                }else{
                    if (bufDescTable[i].dirty) //page modified
                    {
                        bufDescTable[i].file->writePage(bufPool[i]); //write the file to disk
                        bufDescTable[i].dirty = false; //set dirty to false
                    }
                    hashTable->remove(file, bufDescTable[i].pageNo); //remove from hash table
                    bufDescTable[i].Clear(); //clear from the buffer manager
                }
            }
        }
    }

/*
 This function allocates a new page and reads it into the buffer pool.
*/
    void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {

        /* ============== */
        /* YOUR CODE HERE */
        /* ============== */
        std::cout << "allocPage" << std::endl;
        try{
            Page temp = file->allocatePage(); //allocate a new page
            PageId pageNumb=temp.page_number();
            FrameId frameinno;
            allocBuf(frameinno);

            hashTable->insert(file, pageNumb, frameinno);
            bufDescTable[frameinno].Set(file, pageNumb);
            bufPool[frameinno] = temp; //place the new page to the buffer pool
            page= &(bufPool[frameinno]); //returns a pointer to the page itself
            pageNo = pageNumb; //returns the page number

        }catch(...){

        }
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
            bufDescTable[framenumberino].valid=false; //set valid to false
            bufDescTable[framenumberino].Clear(); //clear the frame table
            hashTable->remove(file, PageNo); //remove from hash table
            file->deletePage(PageNo); //delete page from file
        }catch(...){

        }
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
