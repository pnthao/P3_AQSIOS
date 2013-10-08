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
#include <errno.h>
//#include <arpa/inet.h>
using namespace std;
using namespace Execution;

int Communicator::start()
{

	//connect the coordinator
	connectCoordinator();

	//create the listening socket (for query migration request) and send the listening port number to the coordinator
	createListeningSocket();

	//send the initial message telling the coordinator the list of active queries at the node
	char msg[512];
	bzero(msg, 512);

	nodeInfo->serialize(msg, 2);
	isUpdateReady =0;
	pthread_mutex_unlock(&mutexUpdateReady);
	sendMessage(msg);

	//start the thread that is in charge of listening to message from the coordinator and
	// incoming migration requests from other nodes
	pthread_attr_init(&receiver_thread_attr);
	pthread_attr_setdetachstate(&receiver_thread_attr,PTHREAD_CREATE_JOINABLE);
	printf("creating listening thread \n");
	pthread_create(&receiver_thread_id, &receiver_thread_attr, receiving, (void*)this);

	//now start the thread that is in charge of reporting node's status to the coordinator from now on
	pthread_attr_init(&report_thread_attr);
	pthread_attr_setdetachstate(&report_thread_attr,PTHREAD_CREATE_JOINABLE);
	printf("creating reporting thread \n");
	pthread_create(&report_thread_id,&report_thread_attr, report,(void*)this);

	return 0;
}

void Communicator::closeCommunication()
{
	//close the reporting thread and socket
	pthread_mutex_lock(&mutexUpdateReady);

	this->bStopReportThread =1;
	pthread_cond_signal(&condUpdateReady);
	pthread_mutex_unlock(&mutexUpdateReady);

	pthread_join(report_thread_id, NULL);

	close(sockfd_coordinator);

	//close the listening thread and socket
	pthread_mutex_lock(&mutexStopReceiverThread);
	this->bStopReceiverThread = 1;
	pthread_mutex_unlock(&mutexStopReceiverThread);
	pthread_join(receiver_thread_id,NULL);
	close(sockfd_listening);
}

int Communicator::connectCoordinator()
{

	struct sockaddr_in serv_addr;
	struct hostent *server;

	sockfd_coordinator = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd_coordinator < 0){
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

	if (connect(sockfd_coordinator,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0){
		printf("ERROR connecting\n");
		return -1;
	}
	else printf("connected to the coordinator \n");

	return 0;

}

void* Communicator::report(void* arg) //the thread routine
{
	Communicator* communicator = (Communicator*)arg;

	char msg[512];

	pthread_mutex_lock(&communicator->mutexUpdateReady);
	while(communicator->bStopReportThread == 0 && communicator->isUpdateReady ==0){
		pthread_cond_wait(&communicator->condUpdateReady,&communicator->mutexUpdateReady);
		if(communicator->isUpdateReady){
			//copy the node info to prepare to send it to the coordinator
			bzero(msg,512);
			communicator->nodeInfo->serialize(msg,0/*all info*/);
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
	printf("reporting thread is about to exit\n");
	pthread_exit(NULL);
}

void* Communicator::receiving(void * arg){
	Communicator* communicator = (Communicator*)arg;

	//the set of fds to check for incoming messages or connection request
	fd_set readablefds;

	timeval timeout;
	//no blocking
	timeout.tv_sec = 30;
	timeout.tv_usec = 0;

	pthread_mutex_lock(&communicator->mutexStopReceiverThread);
	while(!communicator->bStopReceiverThread){
		pthread_mutex_unlock(&communicator->mutexStopReceiverThread);
		FD_ZERO(&readablefds);
		// add the listening socket to the set
		FD_SET(communicator->sockfd_listening, &readablefds);
		//add the socket connected to the coordinator to the set
		FD_SET(communicator->sockfd_coordinator, &readablefds);
		//check if any socket in the list is readable

		int maxfd = (communicator->sockfd_coordinator > communicator->sockfd_listening)?
				communicator->sockfd_coordinator:communicator->sockfd_listening;

		int activity = select(maxfd, &readablefds,NULL,NULL,&timeout);

		if(activity > 0 ){

			//if there is a coming connection request
			if(FD_ISSET(communicator->sockfd_listening,&readablefds)){
				//creating a connection with the requesting AQSIOS node to initialize the query migration process
				communicator->addMigrationThread(communicator->openMigrationChannel());
			}
			if(FD_ISSET(communicator->sockfd_coordinator, &readablefds))
			{
				//receive and process coordinator's incoming message
			}
		}
		pthread_mutex_lock(&communicator->mutexStopReceiverThread);

	}
	pthread_mutex_unlock(&communicator->mutexStopReceiverThread);
	//wait for the migration handling threads to exit
	communicator->joinAllMigrationThreads();
	printf("receiving thread is about to exit\n");
	pthread_exit(NULL);
}
int Communicator::sendMessage(char* msg)
{
	int n = write(sockfd_coordinator,msg,strlen(msg)+1);
	if (n<0){
		printf("error sending message to coordinator \n");
		return -1;
	}
	else
		return 0;
}
int Communicator::createListeningSocket(){

	sockaddr_in node_addr;
	sockfd_listening = socket(AF_INET, SOCK_STREAM, 0); //stream-based TCP/IP

	if (sockfd_listening < 0){
		printf("ERROR opening socket");
		return -1;
	}
	memset((char*)&node_addr,0,sizeof(node_addr));

	node_addr.sin_family = AF_INET;
	node_addr.sin_addr.s_addr = INADDR_ANY;
	node_addr.sin_port = htons(0); //let the OS pick the available port

	if (bind(sockfd_listening, (sockaddr *) &node_addr,
			sizeof(node_addr)) < 0){
		printf("ERROR on binding listening socket\n");
		return -1;
	}

	//send the listening port number to the coordinator
	socklen_t len = sizeof(node_addr);
	getsockname(sockfd_listening,(sockaddr*)&node_addr, &len);

	char msg[16];
	sprintf(msg,"P,%d,E", node_addr.sin_port);
	sendMessage(msg);
	return 0;
}

pthread_t Communicator::openMigrationChannel(){

	//create a new thread to handle this new migration channel
	pthread_t migration_threadID;
	pthread_attr_t migration_thread_attr;
	pthread_attr_init(&migration_thread_attr);
	pthread_attr_setdetachstate(&migration_thread_attr,PTHREAD_CREATE_JOINABLE);

	printf("creating a new query migration thread \n");
	pthread_create(&migration_threadID, &migration_thread_attr, handleMigration, (void*)this);

	return migration_threadID;
}

void* Communicator::handleMigration(void* arg){

	Communicator* comm = (Communicator*) arg;

	//accept the connection request from the migration source
	socklen_t clilen;
	sockaddr_in cli_addr;

	clilen = sizeof(cli_addr);
	int sockfd_migrationSrc = accept(comm->sockfd_listening,
			(struct sockaddr *) &cli_addr,
			&clilen);
	if (sockfd_migrationSrc < 0){
		printf("ERROR on accept query migration request");
		comm->removeMigrationThread(pthread_self());
		pthread_exit(NULL);
	}
	//else: start the migration communication
	printf("starting query migration...");

	//end
	close(sockfd_migrationSrc);
	comm->removeMigrationThread(pthread_self());
	pthread_exit(NULL);

}

void Communicator::addMigrationThread(pthread_t threadID)
{
	pthread_mutex_lock(&mutex_migrationThreadIDs);
	list<pthread_t>::iterator it;
	for(it = migration_threadIDs.begin(); it!=migration_threadIDs.end(); it++){
			if((*it)==threadID)//already there
				return;
	}
	migration_threadIDs.push_back(threadID);
	pthread_mutex_unlock(&mutex_migrationThreadIDs);
}

void Communicator::removeMigrationThread(thread_t threadID)
{
	pthread_mutex_lock(&mutex_migrationThreadIDs);
	list<pthread_t>::iterator it;
	for(it = migration_threadIDs.begin(); it!=migration_threadIDs.end(); it++){
		if((*it)==threadID){
			migration_threadIDs.erase(it);
			break;
		}
	}
	pthread_mutex_unlock(&mutex_migrationThreadIDs);
}

void Communicator::joinAllMigrationThreads(){
	pthread_mutex_lock(&mutex_migrationThreadIDs);
	list<pthread_t>::iterator it;
	for(it = migration_threadIDs.begin(); it!=migration_threadIDs.end(); it++){
		pthread_join((*it), NULL);
	}
	pthread_mutex_unlock(&mutex_migrationThreadIDs);
}


