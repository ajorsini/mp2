/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->transID = 0;
	this->pendR.clear();
	this->pendCUD.clear();
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	vector<Node> node = findNodes(key);
	string msgBff;
  Message message(++transID, memberNode->addr, CREATE, key, value, PRIMARY);
	for(int i=0; i<3; i++) {
		message.replica = static_cast<ReplicaType>(i);
		msgBff = message.toString();
		emulNet->ENsend(memberNode->addr, node.at(i).nodeAddress, msgBff, msgBff.size());
	}
	pendCUD.emplace_back(transID, par->getcurrtime(), CREATE, key, value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	vector<Node> node = findNodes(key);
	string msgBff;
  Message message(++transID, memberNode->addr, READ, key);
	msgBff = message.toString();
	for(int i=0; i<3; i++)
		emulNet->ENsend(memberNode->addr, node.at(i).nodeAddress, msgBff, msgBff.size());
	pendR.emplace_back(transID, par->getcurrtime(), key);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	vector<Node> node = findNodes(key);
	string msgBff;
  Message message(++transID, memberNode->addr, UPDATE, key, value, PRIMARY);
	for(int i=0; i<3; i++) {
		message.replica = static_cast<ReplicaType>(i);
		msgBff = message.toString();
		emulNet->ENsend(memberNode->addr, node.at(i).nodeAddress, msgBff, msgBff.size());
	}
	pendCUD.emplace_back(transID, par->getcurrtime(), UPDATE, key, value);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	vector<Node> node = findNodes(key);
	string msgBff;
  Message message(++transID, memberNode->addr, DELETE, key);
	msgBff = message.toString();
	for(int i=0; i<3; i++)
		emulNet->ENsend(memberNode->addr, node.at(i).nodeAddress, msgBff, msgBff.size());
	pendCUD.emplace_back(transID, par->getcurrtime(), DELETE, key, "");
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(int transID, string key, string value, ReplicaType replica) {
	vector<Node> node = findNodes(key);
  bool addKey = false;
	for(int i=0; i<3 && !addKey; i++)
		addKey = (memberNode->addr == node.at(i).nodeAddress);
	if(ht->count(key) > 0) addKey = false;
	if(addKey) {
		addKey = ht->create(key, value);
	  logCreateSuccess(&memberNode->addr, false, transID, key, value);
	} else
	  logCreateFail(&memberNode->addr, false, transID, key, value);
	return addKey;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(int transID, string key) {
	vector<Node> node = findNodes(key);
	string v = "";
  bool readKey = false;
	for(int i=0; i<3 && !readKey; i++)
		readKey = (memberNode->addr == node.at(i).nodeAddress);
	if(readKey) {
	  v = ht->read(key);
		if(v != "") logReadSuccess(&memberNode->addr, false, transID, key, v);
	} else
	  logReadFail(&memberNode->addr, false, transID, key);
	return v;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(int transID, string key, string value, ReplicaType replica) {
	vector<Node> node = findNodes(key);
  bool updtKey = false;
	for(int i=0; i<3 && !updtKey; i++)
		updtKey = (memberNode->addr == node.at(i).nodeAddress);
	if(ht->count(key) != 1) updtKey = false;
	if(updtKey) {
		updtKey = ht->update(key, value);
	  logUpdateSuccess(&memberNode->addr, false, transID, key, value);
	} else
	  logUpdateFail(&memberNode->addr, false, transID, key, value);
	return updtKey;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(int transID, string key) {
	vector<Node> node = findNodes(key);
  bool delKey = false;
	for(int i=0; i<3 && !delKey; i++)
		delKey = (memberNode->addr == node.at(i).nodeAddress);
	if(ht->count(key) != 1) delKey = false;
	if(delKey) {
		delKey = ht->deleteKey(key);
	  logDeleteSuccess(&memberNode->addr, false, transID, key);
	} else
	  logDeleteFail(&memberNode->addr, false, transID, key);
	return delKey;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char *data;
	int size;

	while ( !memberNode->mp2q.empty() ) {
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();
		string message(data, data + size);
		Message Msg(message);
    switch(Msg.type) {
			case CREATE:
			  clientCreate(Msg.key, Msg.value);
			  break;
			case UPDATE:
			  clientUpdate(Msg.key, Msg.value);
				break;
			case READ:
  			clientRead(Msg.key);
			  break;
			case DELETE:
			  clientDelete(Msg.key);
				break;
			case REPLY:
			  setPendWrDl(Msg.transID, Msg.success);
				break;
			case READREPLY:
			  setPendRead(Msg.transID, Msg.value);
			  break;
		}
	}

	checkPendRead();
	checkPendWrDl();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}

QuorumStat pendingRead::gotQuorum(int currTime) {
	QuorumStat r;
	n = 0;
	if((currTime - timestamp) < QTMOUT) {
		if(q == 3) r = QSUCCESS;
		else r = QWAIT;
	} else {
		if(q >= 2) r = QSUCCESS;
		else r = QFAIL;
  }
	if(r == QSUCCESS) {
		if(value[0] != "") {
			n++;
			if(value[0] == value[1]) n++;
			if(value[0] == value[2]) n++;
		} else if(value[1] != "") {
			n++;
			if(value[1] == value[2]) n++;
		} else if(value[2] != "") {
			n++;
		}
		if(n < 2) r = QFAIL;
	}
	return r;
}

QuorumStat pendingWrDl::gotQuorum(int currTime) {
	QuorumStat r;
	n = 0;

	if(stblzn) {
		if(q == 0) {
			if((currTime - timestamp) < QTMOUT) r = QWAIT;
			else r = QFAIL;
		} else {
			if(status[0]) r = QSUCCESS;
			else r = QFAIL;
	  }
	} else {
		if((currTime - timestamp) < QTMOUT) {
			if(q == 3) r = QSUCCESS;
			else r = QWAIT;
		} else {
			if(q >= 2) r = QSUCCESS;
			else r = QFAIL;
	  }
		if(r == QSUCCESS) {
			for(int i=0; i<3; i++)
			  if(status[i]) n++;
			if(n < 2) r = QFAIL;
		}
	}
	return r;
}

vector<pendingRead>::iterator MP2Node::findPendRead(int transID) {
	vector<pendingRead>::iterator it;
	for (it = pendR.begin() ; it != pendR.end(); it++)
		if(it->transID == transID) break;
	return it;
}

vector<pendingWrDl>::iterator MP2Node::findPendWrDl(int transID) {
	vector<pendingWrDl>::iterator it;
	for (it = pendCUD.begin() ; it != pendCUD.end(); it++)
		if(it->transID == transID) break;
	return it;
}

void MP2Node::setPendRead(int transID, string value) {
	vector<pendingRead>::iterator it = findPendRead(transID);
	if(it != pendR.end()) pendR.setValue(value);
}

void MP2Node::setPendWrDl(int transID, bool st) {
	vector<pendingWrDl>::iterator it = findPendWrDl(transID);
	if(it != pendCUD.end()) pendCUD.setStatus(st);
}

string pendingRead::getValue() {
  if(value[0] == value[1] || value[0] == value[2]) return value[0];
	if(value[1] == value[2]) return value[1];
}

void MP2Node::checkPendRead() {
	vector<pendingRead>::iterator rit;
	for(rit=pendR.begin(); rit!=pendR.end(); ) {
		switch(rit->gotQuorum()) {
			case QSUCCESS:
				logReadSuccess(&memberNode->addr, true, rit->getTransID(), rit->getKey(), rit->getValue());
				rit = pendR.erase(rit);
				break;
			case QFAIL:
				logReadFail(&memberNode->addr, true, rit->getTransID(), rit->getKey());
				rit = pendR.erase(rit);
				break;
			case QWAIT:
			  rit++
				break;
		}
	}
}

void MP2Node::checkPendWrDl() {
  vector<pendingWrDl>::iterator wit;
	for(wit=pendCUD.begin(); wit!=pendCUD.end(); ) {
		switch(wit->gotQuorum()) {
			case QSUCCESS:
			  switch(wit->getMt) {
					case CREATE:
						logCreateSuccess(&memberNode->addr, true, wit->getTransID(), wit->getKey(), wit->getValue());
						break;
					case UPDATE:
						logUpdateSuccess(&memberNode->addr, true, wit->getTransID(), wit->getKey(), wit->getValue());
						break;
					case DELETE:
						logDeleteSuccess(&memberNode->addr, true, wit->getTransID(), wit->getKey());
						break;
					default:
						break;
				}
				rit = pendCUD.erase(rit);
				break;
			case QFAIL:
				switch(wit->getMt) {
					case CREATE:
						logCreateFail(&memberNode->addr, true, wit->getTransID(), wit->getKey(), wit->getValue());
						break;
					case UPDATE:
						logUpdateFail(&memberNode->addr, true, wit->getTransID(), wit->getKey(), wit->getValue());
						break;
					case DELETE:
						logDeleteFail(&memberNode->addr, true, wit->getTransID(), wit->getKey());
						break;
					default:
						break;
				}
				wit = pendCUD.erase(wit);
				break;
			case QWAIT:
			  wit++
				break;
		}
	}
}
