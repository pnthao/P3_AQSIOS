/*
 * node_info.cc
 *
 *  Created on: Jan 10, 2013
 *      Author: Thao Pham
 */
#ifndef NODE_INFO_H_
#include "execution/communicator/node_info.h"
#endif

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
using namespace std;

int Node_Info::set_capacity(int class_id, double capacity){
	if ((class_id < 0) or (class_id > this->number_of_classes))
		return -1;
	else
		this->capacity[class_id] = capacity;
	return 0;
}

int Node_Info::set_capacity_usage(int class_id, double capacity_usage){
	if ((class_id < 0) or (class_id > this->number_of_classes))
			return -1;
		else
			this->capacity_usage[class_id] = capacity_usage;
	return 0;

}

int Node_Info::set_local_priority(int class_id, double local_priority){
	if ((class_id < 0) or (class_id > this->number_of_classes))
			return -1;
		else
			this->local_priority[class_id] = local_priority;
	return 0;
}

void Node_Info::serialize(char* msg, int msgType){

	//msg format: <msgType>,<capacity list>,<capacity usage list>,<local priority list>
	//header code
	sprintf(msg, "NI,%d,", msgType); //specifying message type
	//supposing the coordinator knows the number of existing classes, no need to embed that info into the msg
	//starting the capacity report
	if(msgType == 0 /*all node info*/||msgType ==1/*class level only*/)
	{	char tempstr[64];
		for(int i=0;i<number_of_classes; i++){
			sprintf(tempstr,"%0.2f,",capacity[i]);
			strcat(msg,tempstr);
		}
		//capacity usage report
		for (int i=0;i<number_of_classes; i++){
			sprintf(tempstr,"%0.2f,",capacity_usage[i]);
			strcat(msg, tempstr);
		}
		//local-priority -- local priority might not need to be reported, since it does not change unless the coordinator request the change
		for (int i=0;i<number_of_classes; i++){
			sprintf(tempstr,"%0.2f,",local_priority[i]);
			strcat(msg, tempstr);
		}
	}
	//active queries and their corresponding query load
	if(msgType ==0 || msgType ==2/*query info only*/){
		std::list<QueryInfo*>::const_iterator it;
		for (it = activeQueries.begin();it != activeQueries.end();it++){
			QueryInfo* qi = *it;
			char temstr[64];
			sprintf(temstr,"%d,%0.2f,",qi->queryID, qi->queryLoad);
			strcat(msg,temstr);
		}
	}
	//end of msg - is it necessary?
	strcat(msg, "E");
	cout<<msg<<endl;
}

int Node_Info::extract(char* msg){

	//need implementation

	return 0;
}

int Node_Info::addActiveQuery(int queryID){
	std::list<QueryInfo*>::const_iterator it;
	for(it=activeQueries.begin();it!=activeQueries.end();it++){
		QueryInfo *qi = *it;
		if(qi->queryID == queryID) //already in the list
			return -1;

	}
	//not found, add the new active query
	QueryInfo* qInfo = new QueryInfo;
	qInfo->queryID = queryID;
	qInfo->queryLoad = 0;
	printf("QID: %d \n",queryID);
	activeQueries.push_back(qInfo);
	return 0;
}

int Node_Info::removeActiveQuery(int queryID){
	std::list<QueryInfo*>::iterator it;
	for(it=activeQueries.begin();it!=activeQueries.end();it++){
		QueryInfo *qi = *it;
		if(qi->queryID == queryID){ //found
			it = activeQueries.erase(it);
			delete qi;
			return 0;
		}
	}
	return -1; //not found
}

int Node_Info::setQueryLoad(int queryID, double load){
	std::list<QueryInfo*>::iterator it;
	for(it=activeQueries.begin();it!=activeQueries.end();it++){
		QueryInfo *qi = *it;
		if(qi->queryID == queryID){ //found
			qi->queryLoad = load;
			return 0;
		}
	}
	return -1; //not found
}
