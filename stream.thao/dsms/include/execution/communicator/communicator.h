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
	char* server_address; //coordinator address
	int server_port; //coordinator port
	int bStopReportThread; //signal to stop from the main thread
	pthread_t report_thread_id;
	pthread_attr_t report_thread_attr;
	int sockfd_coordinator;

	/*the receiver thread, which waits for and receives message from the coordinator
	 * and listens for requests of query migrations (from source node)
	 */
	pthread_t receiver_thread_id;
	pthread_attr_t receiver_thread_attr;
	int sockfd_listening;
	int bStopReceiverThread;

	//the list of IDs of threads that is currently handling query migration
	std::list<pthread_t> migration_threadIDs;
	//mutex to protect the above list
	pthread_mutex_t mutex_migrationThreadIDs;

public:
	int isUpdateReady; //the scheduler will set this variable to inform
						 //the communicator that update is available to be sent to coordinator

	pthread_mutex_t mutexUpdateReady;
	pthread_cond_t condUpdateReady;

	//used for the receiver thread
	pthread_mutex_t mutexStopReceiverThread;

	Node_Info* nodeInfo;
	Communicator(Node_Info* node_info)
	{
		server_address = new char[128];
		bStopReportThread = 0;
		pthread_mutex_init(&mutexUpdateReady, NULL);
		pthread_cond_init(&condUpdateReady,NULL);

		bStopReceiverThread = 0;
		pthread_mutex_init(&mutexStopReceiverThread, NULL);

		pthread_mutex_init(&mutex_migrationThreadIDs, NULL);

		nodeInfo = node_info;
	}

	Communicator(char* srv_address, int srv_port, Node_Info* node_info)
	{
		server_address = new char[128];
		strcpy(server_address,srv_address);
		server_port = srv_port;
		bStopReportThread = 0;
		pthread_mutex_init(&mutexUpdateReady, NULL);
		pthread_cond_init(&condUpdateReady,NULL);

		bStopReceiverThread = 0;
		pthread_mutex_init(&mutexStopReceiverThread, NULL);

		nodeInfo = node_info;
	}
	~Communicator()
	{
		delete []server_address;
		pthread_attr_destroy(&report_thread_attr);
		pthread_attr_destroy(&receiver_thread_attr);


		pthread_mutex_destroy(&mutexUpdateReady);
		pthread_mutex_destroy(&mutexStopReceiverThread);
		pthread_mutex_destroy(&mutex_migrationThreadIDs);
		pthread_cond_destroy(&condUpdateReady);

	}
	int start(); //start connection and create new threads
	//create a socket to listen for incoming requests for query migration from other nodes
	int createListeningSocket();
	void closeCommunication();
	static void* report(void* arg); //run by a child thread, arg should be the this pointer
	static void *receiving(void* arg);//run by another child thread, arg should be the this pointer
	int connectCoordinator();
	int sendMessage(char* msg);
	char* receiveMessage();
	//this function creates a thread to handle migration, and returns the thread_id
	pthread_t openMigrationChannel();
	static void* handleMigration(void* arg);
	void addMigrationThread(pthread_t threadID);
	void removeMigrationThread(pthread_t threadID);
	void joinAllMigrationThreads();


};

}

#endif /* COMMUNICATOR_H_ */
