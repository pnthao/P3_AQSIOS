/*
 * node_info.h
 *
 *  Created on: Jan 10, 2013
 *      Author: Thao Pham
 */

#ifndef NODE_INFO_H_
#define NODE_INFO_H_
#include <list>
using namespace std;
struct QueryInfo{
	int queryID;
	double queryLoad;
};
class Node_Info{
private:
	int number_of_classes;
	double* capacity;
	double* capacity_usage; //list of capacity usage of the classes ( = 0 if the class does not exist in the current node)
	double* local_priority;

public:
	list<QueryInfo*> activeQueries; //list of active queries running on this node
	Node_Info(int number_of_classes)
	{
		this->number_of_classes = number_of_classes;
		capacity = new double[number_of_classes];
		capacity_usage = new double[number_of_classes];
		local_priority = new double[number_of_classes];
	}

	~Node_Info()
	{
		delete[] capacity;
		delete[] capacity_usage;
		delete[] local_priority;
		std::list<QueryInfo*>::iterator it = activeQueries.begin();
		while(it!=activeQueries.end()){
			QueryInfo* qi = *it;
			it = activeQueries.erase(it);
			if(qi)
				delete qi;
		}
	}
	/*serialize the node info into string recognizable to the coordinator.
		msgType = 0: all info
		msgType = 1: class-level info only
		msgType = 2: active query info only
	*/
	void serialize(char* msg, int msgType =0);
	int extract(char * msg); //extract the node info from the msg;
	int set_capacity(int class_id, double capacity); //class_id starts from 0;
	int set_capacity_usage(int class_id, double capacity_usage);
	int set_local_priority(int class_id, double priority);
	int addActiveQuery(int queryID);
	int removeActiveQuery(int queryID);
	int setQueryLoad(int queryID, double load);
};

#endif /* NODE_INFO_H_ */
