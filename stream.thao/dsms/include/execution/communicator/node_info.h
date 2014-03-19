/*
 * node_info.h
 *
 *  Created on: Jan 10, 2013
 *      Author: Thao Pham
 */

#ifndef NODE_INFO_H_
#define NODE_INFO_H_
#include <list>
#include <vector>
using namespace std;
struct QueryInfo{
	int queryID;
	double queryLoad;
};
class Node_Info{
private:
	int number_of_classes;
	vector<double> capacities;
	vector<double> capacity_usages; //list of capacity usage of the classes ( = 0 if the class does not exist in the current node)
	vector<double> local_priorities;

public:
	list<QueryInfo> activeQueries; //list of active queries running on this node
	Node_Info(int number_of_classes)
	{
		this->number_of_classes = number_of_classes;
		for(int i=0;i<number_of_classes;i++){
			capacities.push_back(0.0);
			capacity_usages.push_back(0.0);
			local_priorities.push_back(0.0);
		}
	}

	~Node_Info()
	{
		/*std::list<QueryInfo*>::iterator it = activeQueries.begin();
		while(it!=activeQueries.end()){
			QueryInfo* qi = *it;
			it = activeQueries.erase(it);
			if(qi)
				delete qi;
		}*/
	}
	/*serialize the node info into string recognizable to the coordinator.
		msgType = 0: all info
		msgType = 1: class-level info only
		msgType = 2: active query info only
	*/
	void serialize(char* msg, int msgType =0);
	int set_capacity(int class_id, double capacity); //class_id starts from 0;
	int set_capacity_usage(int class_id, double capacity_usage);
	int set_local_priority(int class_id, double priority);
	int addActiveQuery(int queryID);
	int removeActiveQuery(int queryID);
	int setQueryLoad(int queryID, double load);
};

#endif /* NODE_INFO_H_ */
