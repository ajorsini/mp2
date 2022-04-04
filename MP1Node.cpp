/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < ADDRSZ; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->op = NULL;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() { if(op) delete op; }

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);
	*/

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  op = new Operation(memberNode->addr.addr, (long) 0, (long) 0, ALIVE, emulNet, par, log);
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
		  op->encode(JOINREQ);
#ifdef DEBUGLOG
      sprintf(s, "Trying to join...");
      log->LOG(&memberNode->addr, s);
#endif
      op->send(joinaddr->addr);
    }
    return 1;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
#ifdef DEBUGLOG
    static char s[1024];
    sprintf(s, "Leaving the group...");
    log->LOG(&memberNode->addr, s);
#endif
	memberNode->bFailed = true;
	memberNode->inited = false;
	memberNode->inGroup = false;
    // node is down!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	delete op;
	op = NULL;
	return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {

    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
		MsgTypes r;
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	memberNode->mp1q.pop();
			if((r = op->decode((char *) ptr)) == LEAVE)
			  break;
			if(r == JOINREP)
			  memberNode->inGroup = true;
    }
		if(r == LEAVE) finishUpThisNode();
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types

bool MP1Node::recvCallBack(void *env, char *data, int size ) {
  if(op->decode(data) == LEAVE)
	  finishUpThisNode();
}
*/

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	vector<nodeEntry *> pl;
	vector<nodeEntry *>::iterator it;
	Statuses st;
	char *iAddr;
	int d;

  op->statusEval();

  op->incrmyhb();
  op->initPingList();
	op->encode(PGYPING);
  pl = op->getpingList();
  for(it=pl.begin(); it != pl.end(); it++) {
		st = (*it)->getstatus();
		if(st == DEAD) continue;
		op->send((*it)->getaddr());
		if(st == SUSPECT) {
			if(d == 0) {
				d = 3;
				iAddr = (*it)->getaddr();
				op->encode(IPGYPING, iAddr);
			} else
			  d--;
		}
		if(st == ALIVE) {
			if(d > 0)
				if(--d == 0) op->encode(PGYPING);
		}
	}
  return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr.addr, 0, sizeof(joinaddr.addr));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;
}



/* ------------------------ myClasses Code ----------------------------------------------------
	 --------------------------------------------------------------------------------------------
	 -------------------------------------------------------------------------------------------- */

/* ------------------------- node.cpp ------------------------------ */

nodeEntry::nodeEntry(char *addr, long hb, long myhb, Statuses status, Params *par) {
	this->par = par;
  memcpy(this->addr, addr, sizeof(this->addr));
  this->hb = hb;
  this->myhb = myhb;
	tstamp = par->getcurrtime();
  this->status = status;
  this->prev = this->next = NULL;
}

nodeEntry *nodeEntry::add(nodeEntry *i) {
  if(*i == *this) { cout << "dup!\n"; return this; } // consider update
  if(!this->prev && *i < *this) {  // first
    i->next = this;
    i->prev = NULL;
    this->prev = i;
    return i;
  }
  if(!this->next && *i > *this) {  // last
    i->prev = this;
    i->next = NULL;
    this->next = i;
    return i;
  }
  if(*i < *this) {
    if(*i > *this->prev) {
      i->next = this;
      i->prev = this->prev;
      this->prev->next = i;
      this->prev = i;
      return i;
    } else {
      return this->prev->add(i);
    }
  } else if(*i > *this) {
    if(*i < *this->next) {
      i->prev = this;
      i->next = this->next;
      this->next->prev = i;
      this->next = i;
      return i;
    } else {
      return this->next->add(i);
    }
  }
  cout << "skipped" << endl;
  return this;
}

nodeEntry *nodeEntry::find(char *addr) {
  int i = memcmp(this->addr, addr, sizeof(this->addr));
  if(!i) return this;
  if(i > 0) {
    if(this->prev) {
      if(memcmp(this->prev->addr, addr, sizeof(this->addr)) < 0) return NULL;
      else return this->prev->find(addr);
    } else return NULL;
  }
  if(i < 0) {
    if(this->next) {
      if(memcmp(this->next->addr, addr, sizeof(this->addr)) > 0) return NULL;
      else return this->next->find(addr);
    } else return NULL;
  }
  return NULL;
}

nodeEntry *nodeEntry::unlink() {
  nodeEntry *r = NULL;
  if(this->prev) { this->prev->next = this->next; r = this->next; }
  if(this->next) { this->next->prev = this->prev; r = this->prev; }
  return r;
}

char *nodeEntry::encode(char **b) {
  memcpy(*b, this->addr, sizeof(this->addr));
  *b += sizeof(this->addr);
  memcpy(*b, &this->hb, sizeof(this->hb));
  *b += sizeof(this->hb);
  memcpy(*b, &this->status, sizeof(this->status));
  *b += sizeof(this->status);
  return *b;
}

char *nodeEntry::decode(char **b, Params *par) {
  memcpy(this->addr, *b, sizeof(this->addr));
  *b += sizeof(this->addr);
  memcpy(&this->hb, *b, sizeof(this->hb));
  *b += sizeof(this->hb);
  memcpy(&this->status, *b, sizeof(this->status));
  *b += sizeof(this->status);
	this->par = par;
  this->tstamp = par->getcurrtime();
	this->prev = this->next = NULL;
  return *b;
}

void nodeEntry::show() {
  printf("%u.%u.%u.%u:%u, hb=%ld, myhb=%ld, st=%d, elapt=%d\n",
                             (unsigned char) addr[0],
                             (unsigned char) addr[1],
                             (unsigned char) addr[2],
                             (unsigned char) addr[3],
                             (unsigned short) addr[4],
                             hb, myhb, status,
                             getElapsedt());
}

nodeEntry& nodeEntry::operator =(const nodeEntry &anotherNodeEntry) {
  memcpy(this->addr, anotherNodeEntry.addr, sizeof(this->addr));
  this->hb = anotherNodeEntry.hb;
  this->myhb = anotherNodeEntry.myhb;
  this->status = anotherNodeEntry.status;
  this->tstamp = anotherNodeEntry.tstamp;
	this->par = anotherNodeEntry.par;
  this->prev = anotherNodeEntry.prev;
  this->next = anotherNodeEntry.next;
  return *this;
}

bool nodeEntry::operator ==(const nodeEntry& anotherNodeEntry) {
	return !memcmp(this->addr, anotherNodeEntry.addr, sizeof(this->addr));
}

bool nodeEntry::operator <(const nodeEntry& anotherNodeEntry) {
	return (memcmp(this->addr, anotherNodeEntry.addr, sizeof(this->addr)) < 0);
}

bool nodeEntry::operator >(const nodeEntry& anotherNodeEntry) {
	return (memcmp(this->addr, anotherNodeEntry.addr, sizeof(this->addr)) > 0);
}

/* ----------------------- end node.cpp ---------------------------- */

/* ------------------------- op.cpp ------------------------------ */

Operation::Operation(char *addr, long hb, long myhb, Statuses status, EmulNet *emulNet, Params *par, Log *log) {
  this->me = new nodeEntry(addr, hb, myhb, status, par);
  this->recsz = sizeof(addr)+sizeof(hb)+sizeof(status);
  this->peersList = this->first = this->last = NULL;
	this->emulNet = emulNet;
	this->par = par;
	this->log = log;
	this->from = new Address;
	this->to = new Address;
	this->me->getAddress(this->from);
}

void Operation::destroyPeersList(nodeEntry *n) {
  nodeEntry *x;
  while((x = n->getNext())) {
    delete n;
    n = x;
  }
}

Operation::~Operation() {
  destroyPeersList(first);
  delete me;
  pingList.clear();
  gossipList.clear();
	if(from) delete from;
	if(to) delete to;
}

void Operation::initPingList() {
  nodeEntry *n = this->first;
  random_device rd;
  pingList.clear();
  while(n) {
    pingList.push_back(n);
    n = n->getNext();
  }
  mt19937 generator(rd());
  shuffle(pingList.begin(), pingList.end(), generator);
}

char *Operation::getHeader(MsgTypes t, char *iAddr) {
  char *b = msgBff;
  memcpy(b, &t, sizeof(t)); b+=sizeof(t);
  me->encode(&b);
  if(iAddr) {
    memcpy(b, iAddr, ADDRSZ);
    b+=ADDRSZ;
  }
  return b;
}

int Operation::updtGossipLst(int n) {
  nodeEntry *p, *b;
  vector<nodeEntry *>::iterator it;
  int i;

  for(it=gossipList.begin() ; it != gossipList.end(); )
    if((*it)->getElapsedt() > TGOSSIPENTRY) it = gossipList.erase(it);
    else it++;
  if(!peersList) return 0;
  p = b = peersList;
  if((i = gossipList.size()) > n) {
    gossipList.erase(gossipList.begin()+n, gossipList.end());
    peersList = gossipList.back();
    i = n;
  } else {
    while(i<n) {
      if(p->isLast()) p = first; // reached the end
      else p = p->getNext();
      if(p == b) break;           // whole round
      if(find(gossipList.begin(), gossipList.end(), p)==gossipList.end()) {
        gossipList.push_back(p);
        i++;
      }
    }
    peersList = p;
  }

  return i;
}

char *Operation::addPayload(char **b) {
  int n = updtGossipLst(((MAXMSGSZ - sizeof(n) - (*b - msgBff)) / recsz));
  vector<nodeEntry *>::iterator it;
  memcpy(*b, &n, sizeof(n)); *b += sizeof(n);
  for(it=gossipList.begin() ; it != gossipList.end(); it++)
    (*it)->encode(b);
  return *b;
}

void Operation::updatePeersList(nodeEntry *n) {
  nodeEntry *x = NULL;
  bool gssp = FALSE;
	Address a, b;
  if(peersList) x = peersList->find(n->getaddr());
  if((gssp = !x)) {
    x = new nodeEntry();
    *x = *n;
    x->setmyhb(me->getmyhb());
    if(peersList) peersList->add(x);
    else peersList = x;
    if(x->isFirst()) first = x;
    if(x->isLast()) last = x;
		log->logNodeAdd(me->getAddress(&a), n->getAddress(&b));
  } else {
    if((gssp = x->sethb(n->gethb()))) {
			x->setstatus(n->getstatus());
			x->setmyhb(me->getmyhb());
			if(n->getstatus() == DEAD)
				log->logNodeRemove(me->getAddress(&a), n->getAddress(&b));
		}
  }
  if(gssp)
	  if(find(gossipList.begin(), gossipList.end(), x)==gossipList.end())
	    gossipList.insert(gossipList.begin(), x);
}

void Operation::showPeersList() {
  nodeEntry *p = first;
  int i = 0;
  cout << "\nPeers List ------------------------------------\n";
  cout << "me: ";
  me->show();
  while(p) {
    cout << i++ << ": ";
    p->show();
    p = p->getNext();
  }
}

void Operation::showPingList() {
	vector<nodeEntry *>::iterator it;
  int i=0;
  cout << "\nPing List ------------------------------------\n";
	cout << "me: ";
  me->show();
  for(it=pingList.begin() ; it != pingList.end(); it++) {
    cout << i++ << ": ";
    (*it)->show();
  }
}

void Operation::showGossipList() {
  vector<nodeEntry *>::iterator it;
  int i=0;
  cout << "\nGossip List ------------------------------------\n";
  for(it=gossipList.begin() ; it != gossipList.end(); it++) {
    cout << i++ << ": ";
    (*it)->show();
  }
}

void Operation::showMsgDir() {
	char *f = from->addr, *t=to->addr;
	printf("%u.%u.%u.%u:%u -> %u.%u.%u.%u:%u\n",
														 (unsigned char) f[0],
														 (unsigned char) f[1],
														 (unsigned char) f[2],
														 (unsigned char) f[3],
														 (unsigned short) f[4],
														 (unsigned char) t[0],
														 (unsigned char) t[1],
														 (unsigned char) t[2],
														 (unsigned char) t[3],
														 (unsigned short) t[4]);
}


size_t Operation::encode(MsgTypes t, char *iAddr) {
  char *b;

  b = getHeader(t, iAddr);
	switch(t) {
    case JOINREP:
    case PGYPING:
    case PGYPONG:
    case IPGYPING:
    case IPGYPONG:
      b = addPayload(&b);
      break;
    default:
      break;
  }
	msgSz = (size_t) (b - msgBff);
  return msgSz;
}

MsgTypes Operation::decode(char *iMsg) {
  char *b = iMsg, iAddr[ADDRSZ];
  bool pyld=FALSE;
  int nr=0, i;
  MsgTypes t;
  nodeEntry n, l;
	Address x, y;

  memcpy(&t, b, sizeof(t)); b+=sizeof(t);
	n.decode(&b, par);
  switch(t) {
		case JOINREQ:
		  encode(JOINREP);
			send(n.getaddr());
		  break;
    case PING:
			encode(PONG);
			send(n.getaddr());
			break;
    case PONG:
      break;
    case IPING:
			memcpy(iAddr, b, ADDRSZ); b+=ADDRSZ;
			encode(IPONG, n.getaddr());
			send(iAddr);
			break;
    case IPONG:
		  memcpy(iAddr, b, ADDRSZ); b+=ADDRSZ;
			encode(PONG);
			send(iAddr);
      break;
    case PGYPING:
			encode(PGYPONG);
			send(n.getaddr());
		case JOINREP:
    case PGYPONG:
      pyld = TRUE;
      break;
    case IPGYPING:
			memcpy(iAddr, b, ADDRSZ); b+=ADDRSZ;
			encode(IPGYPONG, n.getaddr());
			send(iAddr);
			pyld = TRUE;
			break;
    case IPGYPONG:
			memcpy(iAddr, b, ADDRSZ); b+=ADDRSZ;
			encode(PGYPONG);
			send(iAddr);
      pyld = TRUE;
      break;
		default:
		  break;
  }
	updatePeersList(&n);
  if(pyld) {
    memcpy(&nr, b, sizeof(nr)); b+=sizeof(nr);
    for(i=0; i<nr; i++) {
      l.decode(&b, par);
			if(l == *me) {
				switch(l.getstatus()) {
					case SUSPECT:     // I'm suspected!
						encode(PING);
						send(n.getaddr());
					  break;
					case DEAD:        // I'm dead!
					  t = LEAVE;
					  log->logNodeRemove(me->getAddress(&x), l.getAddress(&y));
						break;
					default:
					  break;
				}
				if(t == LEAVE) break;
			} else {
      	updatePeersList(&l);
      }
    }
  }
	return t;
}

void Operation::statusEval() {
	nodeEntry *n = this->first;
	Address a, b;
	while(n) {
		if(n->getstatus() == DEAD) {
			n = n->getNext();
			continue;
		}
		if(n->getElapsedt() >= TFAIL) {
			if(n->getElapsedt() >= TREMOVE) {
				n->setstatus(DEAD);
				log->logNodeRemove(me->getAddress(&a), n->getAddress(&b));
			} else {
				n->setstatus(SUSPECT);
			}
			n->setmyhb(me->getmyhb());
			if(find(gossipList.begin(), gossipList.end(), n)==gossipList.end())
			  gossipList.insert(gossipList.begin(), n);
		}
		n = n->getNext();
	}
}

/* ----------------------- end op.cpp ---------------------------- */
