/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#define QTMOUT 3
#define DELKEYTMOUT 4

enum QuorumStat { QFAIL, QWAIT, QSUCCESS };

class pendingRead {
private:
	int transID, q, timestamp;
	string key, value[3];
public:
	pendingRead(int transID, int timestamp, string key) { this->transID = transID; this->timestamp = timestamp; this->key = key; q = 0; value = {"", "", ""}; }
	~pendingRead() {};
	int getTransID() { return transID; };
	int setValue(string v) { value[q++] = v; return q; };
	QuorumStat gotQuorum(int currTime);
	string getKey() { return key; };
	string getValue();
};

class pendingWrDl {
private:
	int transID, q, timestamp;
	MessageType mt;
	bool status[3], stblzn;
	string key, value;
public:
	pendingWrDl(int transID, MessageType mt, int timestamp, string key, string value) { this->transID = transID; this->mt = mt; this->timestamp = timestamp; this->key = key; this->value = value; q = 0; status = {false, false, false}; stblzn = false; }
	~pendingWrDl() {};
	int getTransID() { return transID; };
	int setStatus(bool v) { status[q++] = v; return q; };
	QuorumStat gotQuorum(int currTime);
	bool isStblzn() { return stblzn; };
	void setStblzn() { stblzn = true; };
	MessageType getMt() { return mt; };
	string getKey() { return key; };
	string getValue() { return value; };
};

class TbDelKey {
private:
	string key;
	int timestamp;
public:
	TbDelKey(string key, int timestamp) { this->key = key; this->timestamp = timestamp; };
	~TbDelKey() {};
	string getKey() { return key; };
	bool delReady(int currTime) { return ((currTime - timestamp) > DELKEYTMOUT); };
};

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable *ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet *emulNet;
	// Object of Log
	Log *log;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member *getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(int transID, string key, string value, ReplicaType replica);
	string readKey(int transID, string key);
	bool updateKeyValue(int transID, string key, string value, ReplicaType replica);
	bool deletekey(int transID, string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	// My public variables & methods
	int transID;
	vector<pendingRead> pendR;
	vector<pendingWrDl> pendCUD;
	vector<TbDelKey> tbDelKey;
	int getTimeStamp { return par->getcurrtime(); };
	vector<pendingRead>::iterator findPendRead(int transID);
	vector<pendingWrDl>::iterator findPendWrDl(int transID);
	void setPendRead(int transID, string value);
	void setPendWrDl(int transID, bool st);
	void checkPendRead();
	void checkPendWrDl();
	vector<node> findNewNodes(string key, vector<Node> newRing);
	bool isListed(Node *n, vector<Node> nList);
	void stblznCreate(string key, string value, Node *node);

  // Destructor
	~MP2Node();
};

#endif /* MP2NODE_H_ */
