/*
 * communicator.cc
 *
 *  Created on: Sep 25, 2012
 *      Author: Thao Pham
 */
#ifndef COMMUNICATOR_H_
#include "execution/communicator/communicator.h"
#endif

#include <thread_db.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <netdb.h>
using namespace std;
using namespace Execution;

int Communicator::start()
{
	int rc =0;
	return rc;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	printf("creating thread \n");
	pthread_create(&thread_id,&attr, communicate,(void*)this);

	return rc;
}

void Communicator::closeCommunication()
{
	pthread_mutex_lock(&mutexUpdateReady);

	this->bStopThread =1;
	pthread_cond_signal(&condUpdateReady);
	pthread_mutex_unlock(&mutexUpdateReady);

	pthread_join(thread_id, NULL);

	close(sockfd);
}

int Communicator::connectCoordinator()
{

	struct sockaddr_in serv_addr;
	struct hostent *server;

	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0){
		printf("ERROR opening socket\n");
		return -1;
	}
	server = gethostbyname(this->server_address);
	if (server == NULL) {
		printf("ERROR, no such host\n");
		return -1;
	}

	bzero((char *) &serv_addr, sizeof(serv_addr));

	serv_addr.sin_family = AF_INET;
	bcopy((char *)server->h_addr,(char *)&serv_addr.sin_addr.s_addr, server->h_length);
	serv_addr.sin_port = htons(this->server_port);

	if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0){
		printf("ERROR connecting\n");
		return -1;
	}
	else printf("connected to the coordinator \n");

	return 0;

}

void* Communicator::communicate(void* arg) //the thread routine
{
	Communicator* communicator = (Communicator*)arg;

	communicator->connectCoordinator();

	char msg[256];

	//send the initial message telling the coordinator the list of active queries at the node

	pthread_mutex_lock(&communicator->mutexUpdateReady);
	while(communicator->bStopThread == 0 && communicator->isUpdateReady ==0){
		pthread_cond_wait(&communicator->condUpdateReady,&communicator->mutexUpdateReady);
		if(communicator->isUpdateReady){
			//copy the node info to prepare to send it to the coordinator
			bzero(msg,256);
			communicator->nodeInfo->serialize(msg);
			communicator->isUpdateReady = 0;
			//unlock the mutexUpdateReady after the information is serialized, release the lock
			//so that the main thread is not blocked during communication with the coordinator
			pthread_mutex_unlock(&communicator->mutexUpdateReady);
			//send the info update to coordinator

			if(communicator->sendMessage(msg)< 0)
				break;

			//lock the UpdateReady mutex again before checking for the value of isUpdateReady
			pthread_mutex_lock(&communicator->mutexUpdateReady);
		}
	}
	pthread_mutex_unlock(&communicator->mutexUpdateReady);
	printf("thread is about to exit\n");
	pthread_exit(NULL);
}

int Communicator::sendMessage(char* msg)
{
	int n = write(sockfd,msg,strlen(msg)+1);
	if (n<0){
		printf("error sending message to coordinator \n");
		return -1;
	}
	else
		return 0;
}





