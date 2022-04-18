/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include <random>
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define TGOSSIPENTRY 3
#define ADDRSZ 6
#define FALSE 0
#define TRUE 1
#define MAXMSGSZ 512


/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types

enum MsgTypes{
    JOINREQ,
    JOINREP,
    DUMMYLASTMSGTYPE
};


 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message

typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

*/

/* ------------------------------- myClasses ---------------------------- */

enum MsgTypes {
    JOINREQ,
    JOINREP,
    PING,
    PONG,
    IPING,
    IPONG,
    PGYPING,
    PGYPONG,
    IPGYPING,
    IPGYPONG,
    LEAVE
};

typedef enum Statuses {
  ALIVE,
  SUSPECT,
  DEAD
} Statuses;

class nodeEntry {
private:
  char addr[ADDRSZ];
  long hb, myhb;
  int tstamp;
  Statuses status;
  Params *par;
  nodeEntry *prev, *next;
public:
  nodeEntry() {};
  nodeEntry(char *addr, long hb, long myhb, Statuses status, Params *par);
  nodeEntry(char **b, Params *par, long myhb=0) { decode(b, par); setmyhb(myhb); };
  ~nodeEntry() {};
  nodeEntry *add(nodeEntry *i);
  nodeEntry *find(char *addr);
  nodeEntry *unlink();
  nodeEntry *getNext() { return this->next; };
  nodeEntry *getPrev() { return this->prev; };
  char *getaddr() { return this->addr; };
  Address *getAddress(Address *a) {
  	memcpy(a->addr, this->addr, sizeof(this->addr));
  	return a;
  };
  long gethb() { return this->hb; };
  long getmyhb() { return this->myhb; };
  Statuses getstatus() { return this->status; };
  int gettstamp() { return this->tstamp; };
  int getElapsedt() { return (par->getcurrtime() - this->tstamp); };
  void setaddr(char *addr) {
    memcpy(this->addr, addr, sizeof(this->addr));
  }
  bool sethb(long hb) {
    bool r = (this->hb < hb);
    if(r) {
      this->hb = hb;
      this->tstamp = par->getcurrtime();
    }
    return r;
  }
  bool setmyhb(long myhb) {
    this->myhb = myhb;
    return TRUE;
  }
  long myhbinc() {
    this->myhb++;
    return this->myhb;
  }
  bool setstatus(Statuses status) {
    bool r = (this->status != status);
    if(r) this->status = status;
    return r;
  }
  char *encode(char **b);
  char *decode(char **b, Params *par);
  void show();
  bool isFirst() { return !this->prev; }
  bool isLast() { return !this->next; }
  nodeEntry& operator =(const nodeEntry &anotherNodeEntry);
  bool operator ==(const nodeEntry &anotherNodeEntry);
  bool operator <(const nodeEntry &anotherNodeEntry);
  bool operator >(const nodeEntry &anotherNodeEntry);
};

class Operation {
private:
  nodeEntry *peersList, *me, *first, *last;
  Member *memberNode;
  EmulNet *emulNet;
  vector<nodeEntry *> pingList, gossipList;
  size_t recsz, msgSz;
  char msgBff[MAXMSGSZ];
  Address *from, *to;
  Params *par;
  Log *log;

public:
  Operation() {};
  Operation(Member *memberNode, Statuses status, EmulNet *emulNet, Params *par, Log *log);
  ~Operation();
  void destroyPeersList(nodeEntry *n);
  void initPingList();
  char *getHeader(MsgTypes t, char *iAddr=NULL);
  int updtGossipLst(int n);
  char *addPayload(char **b);
  void updatePeersList(nodeEntry *n);
  Address *getToAddress(char *a) {
  	memcpy(to->addr, a, sizeof(to->addr));
  	return to;
  };
  void showPeersList();
  void showGossipList();
  void showPingList();
  void showMsgDir();
  void statusEval();
  int incrmyhb() { me->sethb(me->myhbinc()); return me->getmyhb(); };
  size_t encode(MsgTypes t, char *iAddr=NULL);
  MsgTypes decode(char *iMsg);
  void send(char *addr) {
    emulNet->ENsend(from, getToAddress(addr), msgBff, msgSz);
  };
  vector<nodeEntry *> getpingList() { return pingList; };
  vector<nodeEntry *> getgossipList() { return gossipList; };
  vector<MemberListEntry>::iterator _memberList_add(char *addr, long hb, long ts);
  vector<MemberListEntry>::iterator _memberList_search(char *addr);
  vector<MemberListEntry>::iterator _memberList_del(char *addr);
};


/* ------------------------------- end myClasses ---------------------------- */

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[ADDRSZ];
  Operation *op;

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
//	void initMemberListTable(Member *memberNode);
	void printAddress(Address *addr);
	virtual ~MP1Node();
};

#endif /* _MP1NODE_H_ */
