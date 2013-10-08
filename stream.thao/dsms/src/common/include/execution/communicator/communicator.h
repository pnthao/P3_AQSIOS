/*
 * communicator.h
 *
 *  Created on: Sep 25, 2012
 *      Author: thao
 */

#ifndef COMMUNICATOR_H_
#define COMMUNICATOR_H_
#endif /* COMMUNICATOR_H_ */

#include <string.h>
#include <stdio.h>
#include<thread_db.h>

namespace Execution{

class Communicator{
private:
	char* server_address;
	int server_port;
	int bStopReportThread;
	pthread_t report_thread_id;
public:
	Communicator()
	{
		server_address = new char[128];
		bStopReportThread = 0;
	}

	Communicator(char* srv_address, int srv_port)
	{
		server_address = new char[128];
		strcpy(server_address,srv_address);
		server_port = srv_port;
		bStopReportThread = 0;
	}
	~Communicator()
	{
		delete []server_address;
	}
	int start(); //start connection and create a new thread
	void closeCommunication();
	static void* report(void* arg); //run by a child thread, arg is the the this pointer
protected:
	int connectCoordinator();
	int sendMessage();
	int receiveMessage();

};

}
