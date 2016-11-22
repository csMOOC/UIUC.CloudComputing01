/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

Address getAddressFromIPAndPort(int ip, short port)
{
    Address address;
    address.init();
    memcpy(&address.addr[0], &ip, sizeof(int));
    memcpy(&address.addr[sizeof(int)], &port, sizeof(short));
    return address;
}


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

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
	 */
	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        addOrUpdateMember(memberNode->addr, memberNode->heartbeat);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    initMemberListTable(memberNode);
    memberNode->inGroup = false;
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
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *) data;
    char *msgBody = (char *)(msg+1);
    Address senderAddr;
    long senderHeartbeat;
    switch (msg->msgType) {
        case JOINREQ:
            // create JOINREP message
            memcpy(&senderAddr, (char *)(msg+1), sizeof(memberNode->addr.addr));
            memcpy(&senderHeartbeat, (char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), sizeof(long));
            addOrUpdateMember(senderAddr, senderHeartbeat);
            sendJoinRepMsg(senderAddr);
            break;
        case JOINREP:
            memberNode->inGroup = true;
            updateMemberList(msgBody);
            break;
        case UPDATEMEMLIST:
            updateMemberList(msgBody);
            break;
        default:
            return false;
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    increaseSelfHeartbeat();
	deleteFailedNodes();
    sendMemberListToGroup();

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

    memset(&joinaddr, 0, sizeof(Address));
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

void MP1Node::sendJoinRepMsg(Address toAddress)
{
    sendMemberListToMember(toAddress, JOINREP);
}

void MP1Node::sendMemberListToGroup()
{
    long currentTime = par->getcurrtime();

	for (int i = 0; i < TPROP; i++) {

		/* initialize random seed: */
		srand (currentTime);

		/* generate secret number between 1 and 10: */
		int idx = rand() % memberNode->memberList.size();

	    if (currentTime - memberNode->memberList[idx].timestamp >= TFAIL)
	    {
	        continue;
	    }
	    Address addr = getAddressFromIPAndPort(memberNode->memberList[idx].id,
			memberNode->memberList[idx].port);
	    sendMemberListToMember(addr, UPDATEMEMLIST);
	}
}

void MP1Node::updateMemberList(char *data)
{
    int numOfMembers;
    memcpy(&numOfMembers, (char *)data, sizeof(int));
    MemberListEntry* memberListEntries = (MemberListEntry*) (data + sizeof(int));
    for (int i = 0; i < numOfMembers; ++i)
    {
        Address address = getAddressFromIPAndPort(memberListEntries[i].id,
                                                  memberListEntries[i].port);
        addOrUpdateMember(address, memberListEntries[i].heartbeat);
    }
}

void MP1Node::addOrUpdateMember(Address memberAddr, long heartbeat)
{
	int id = *(int*)(&memberAddr.addr[0]);
	int port = *(short*)(&memberAddr.addr[4]);
    bool memberFound = false;

    for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
            iter != memberNode->memberList.end(); ++iter)
    {
        if (iter->id == id && iter->port == port)
        {
            memberFound = true;
            if (iter->heartbeat < heartbeat)
            {
                iter->heartbeat = heartbeat;
                iter->timestamp = par->getcurrtime();
            }
            break;
        }
    }

    if (!memberFound)
    {
        memberNode->memberList.push_back(
            MemberListEntry(id, port, heartbeat, par->getcurrtime()));
        memberNode->nnb += 1;
        log->logNodeAdd(&memberNode->addr, &memberAddr);
    }
}

void MP1Node::increaseSelfHeartbeat()
{
    ++memberNode->heartbeat;
    for (vector<MemberListEntry>::iterator iter = memberNode->memberList.begin();
            iter != memberNode->memberList.end(); ++iter)
    {
        Address addr = getAddressFromIPAndPort(iter->id, iter->port);
        if (addr == memberNode->addr)
        {
            iter->heartbeat = memberNode->heartbeat;
            iter->timestamp = par->getcurrtime();
            break;
        }
    }
}

void MP1Node::deleteFailedNodes()
{
    long currentTime = par->getcurrtime();
    for (int i = memberNode->memberList.size() - 1; i >= 0; i--)
    {
        if (currentTime - memberNode->memberList[i].timestamp >= TREMOVE)
        {
            Address addr = getAddressFromIPAndPort(memberNode->memberList[i].id,
				memberNode->memberList[i].port);
            log->logNodeRemove(&memberNode->addr, &addr);
            memberNode->memberList.erase(begin(memberNode->memberList) + i);
        }
    }
}

void MP1Node::sendMemberListToMember(Address toAddress, MsgTypes msgType)
{
    int numOfMembers = memberNode->memberList.size();
    size_t msgsize = sizeof(MessageHdr) + sizeof(long) + numOfMembers * sizeof(MemberListEntry);
    MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREP message: format of data is {struct MemberListEntry}
    msg->msgType = msgType;
    memcpy((char *)(msg+1), &numOfMembers, sizeof(int));
    char *p = (char *)(msg+1) + sizeof(int);
    for (vector<MemberListEntry>::const_iterator iter = memberNode->memberList.begin();
            iter != memberNode->memberList.end(); ++iter)
    {
        memcpy(p, &(*iter), sizeof(MemberListEntry));
        p += sizeof(MemberListEntry);
    }

    // send JOINREQ message to introducer member
    emulNet->ENsend(&memberNode->addr, &toAddress, (char *)msg, msgsize);

    free(msg);
}
