/*
 * communicator.h
 *
 *  Created on: Sep 25, 2012
 *      Author: Thao Pham
 */

#ifndef COMMUNICATOR_H_
#define COMMUNICATOR_H_


#include <string.h>
#include <stdio.h>
#include<thread_db.h>

#ifndef NODE_INFO_H_
#include "execution/communicator/node_info.h"
#endif

namespace Execution{

class Communicator{
private:
	char* server_address;
	int server_port;
	int bStopThread;
	pthread_t thread_id;
	pthread_attr_t attr;
	int sockfd;

public:
	int isUpdateReady; //the scheduler will set this variable to inform
						 //the communicator that update is available to be sent to coordinator

	pthread_mutex_t mutexUpdateReady;
	pthread_cond_t condUpdateReady;
	Node_Info* nodeInfo;
	Communicator(Node_Info* node_info)
	{
		server_address = new char[128];
		bStopThread = 0;
		pthread_mutex_init(&mutexUpdateReady, NULL);
		pthread_cond_init(&condUpdateReady,NULL);
		nodeInfo = node_info;
	}

	Communicator(char* srv_address, int srv_port, Node_Info* node_info)
	{
		server_address = new char[128];
		strcpy(server_address,srv_address);
		server_port = srv_port;
		bStopThread = 0;
		pthread_mutex_init(&mutexUpdateReady, NULL);
		pthread_cond_init(&condUpdateReady,NULL);
		nodeInfo = node_info;
	}
	~Communicator()
	{
		delete []server_address;
		pthread_attr_destroy(&attr);
		pthread_mutex_destroy(&mutexUpdateReady);
		pthread_cond_destroy(&condUpdateReady);
	}
	int start(); //start connection and create a new thread
	void closeCommunication();
	static void* communicate(void* arg); //run by a child thread, arg should be the this pointer
	int connectCoordinator();
	int sendMessage(char* msg);
	char* receiveMessage();


};

}

#endif /* COMMUNICATOR_H_ */
