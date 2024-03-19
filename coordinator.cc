#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"
#include<glog/logging.h>

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};

//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here
        int clusterId = serverinfo->clusterid();
        log(INFO,"Received heartbeat from server " + std::to_string(serverinfo->serverid()) + " to cluster " + std::to_string(clusterId));
        if (clusters[clusterId].size() == 0) {
            // cluster does not exist, create a new znode
            zNode * currZ = new zNode();
            currZ->hostname = serverinfo->hostname();
            currZ->last_heartbeat = getTimeNow();
            currZ->port = serverinfo->port();
            currZ->serverID = serverinfo->serverid();   
            // push back new server in the cluster vector         
            clusters[clusterId].push_back(currZ);
            confirmation->set_status(false);
        } else {
            // if cluster exists, set new time for last heartbeat on the server
            zNode * currZ = clusters[clusterId][clusters[clusterId].size()-1];
            currZ->last_heartbeat = getTimeNow();
            confirmation->set_status(true);
        }
        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    // void printServerInfo(ServerInfo* serverInfo) {
    //     std::cout << serverInfo->serverid() << std::endl;
    //     std::cout << serverInfo->clusterid() << std::endl;
    //     std::cout << serverInfo->hostname() << std::endl;
    //     std::cout << serverInfo->port() << std::endl;
    // }
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // Your code here
        // calculate the cluster that the user should use based on the given formula
        int userId = id->id();
        int clusterId = ((userId - 1) % 3) + 1;
        int serverId = 1;
        // assign the user to the last created server on the cluster if there exists a server and it is active
        log(INFO,"Assigning user " + std::to_string(userId) + " to cluster " + std::to_string(clusterId));
        if (clusters[clusterId].size() != 0 && clusters[clusterId][clusters[clusterId].size()-1]->isActive()) {
            serverinfo->set_serverid(serverId);
            serverinfo->set_clusterid(clusterId);
            serverinfo->set_hostname(clusters[clusterId][clusters[clusterId].size()-1]->hostname);
            serverinfo->set_port(clusters[clusterId][clusters[clusterId].size()-1]->port);
        } else {
            // if there is no active server, return server unavailable
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server unavailable"); 
        }
        return Status::OK;
    }
    // function to check if there exists a server that the client can call on their cluster
    Status exists(ServerContext* context, const ID* id, ServerInfo* serverinfo)  {
    
        // find the cluster that the user belongs on
        int userId = id->id();
        int clusterId = ((userId - 1) % 3) + 1;
        int serverId = 1;
        // check if the last created server on the cluster has sent a heartbeat recently, if so set to active
        log(INFO,"Checking if server " + std::to_string(serverId) + " on cluster " + std::to_string(clusterId) + " exists");
        if (clusters[clusterId].size() != 0 && difftime(getTimeNow(),clusters[clusterId][clusters[clusterId].size()-1]->last_heartbeat)<1) {
            serverinfo->set_active(true);
        } else {
            serverinfo->set_active(false);
        }
        return Status::OK;
    }

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    std::string log_file_name = std::string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Coordinator starting...");
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(difftime(getTimeNow(),s->last_heartbeat)>10){
                    log(WARNING,"Missed heartbeat from server");
                    std::cout << "missed heartbeat from server " << s->serverID << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

