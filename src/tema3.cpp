#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <list>

#define NumberCluster 4
using namespace std;

//citirea din fisier si trimis rank cluster
vector<int> send_to_coord(int rank) {
    vector<int> workers;
    char file_name[15];
    sprintf(file_name, "cluster%d.txt", rank);
    int num_workers;
    FILE *fp = fopen(file_name, "r");
	fscanf(fp, "%d", &num_workers);
    int data;
    workers.push_back(rank);
	for (int i = 0; i < num_workers; i++) {
		fscanf(fp, "%d", &data);
        workers.push_back(data);
    }
    for(int i = 1; i <= num_workers; i++) {
        std::cout << "M(" << rank << "," << workers.at(i) <<")\n";
        MPI_Send(&rank, 1, MPI_INT, workers.at(i), 0, MPI_COMM_WORLD);
    }
    return workers;
}

//print cluster
void print_cluster(int rank, vector<vector<int>>cluster) {
    std::cout << rank <<" ->";
    for(long unsigned int i = 0; i < 4; ++i) {
        if(cluster.at(i).empty() == false) {
            std::cout << " " << cluster.at(i).at(0)<< ":";
            for(long unsigned int j = 1; j < cluster[i].size(); ++j) {
                std::cout << cluster[i][j];
                if(j != cluster[i].size() - 1) {
                    std::cout << ",";
                }
            }
        }
    }
    std::cout << "\n";
}

//trimit workerii cunoscuti de fiecare cluster 0->3->2->1
vector<vector<int>> send_worker(int rank, vector<vector<int>> cluster) {
    MPI_Status status;
    if(rank == 0) {
        int size = cluster[0].size();
        MPI_Send(&size, 1, MPI_INT, 3, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[0][0], size, MPI_INT, 3, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",3)\n";
    }
    if(rank == 3) {
        int number_work;
        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv(number_work);
        MPI_Recv(&workers_recv[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv[0]] = workers_recv;

        int size = cluster[0].size();
        MPI_Send(&size, 1, MPI_INT, 2, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[0][0], size, MPI_INT, 2, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",2)\n";

        size = cluster[3].size();
        MPI_Send(&size, 1, MPI_INT, 2, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[3][0], size, MPI_INT, 2, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",2)\n";

    }
    if(rank == 2) {
        int number_work;
        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv(number_work);
        MPI_Recv(&workers_recv[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv[0]] = workers_recv;

        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv2(number_work);
        MPI_Recv(&workers_recv2[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv2[0]] = workers_recv2;

        int size = cluster[0].size();
        MPI_Send(&size, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[0][0], size, MPI_INT, 1, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",1)\n";
        size = cluster[3].size();
        MPI_Send(&size, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[3][0], size, MPI_INT, 1, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",1)\n";

        size = cluster[2].size();
        MPI_Send(&size, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
        MPI_Send(&cluster[2][0], size, MPI_INT, 1, 1, MPI_COMM_WORLD);
        std::cout << "M(" << rank << ",1)\n";
    }
    if(rank == 1) {
        int number_work;
        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv(number_work);
        MPI_Recv(&workers_recv[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv[0]] = workers_recv;

        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv2(number_work);
        MPI_Recv(&workers_recv2[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv2[0]] = workers_recv2;

        MPI_Recv(&number_work, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        vector<int> workers_recv3(number_work);
        MPI_Recv(&workers_recv3[0], number_work, MPI_INT, status.MPI_SOURCE, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv3[0]] = workers_recv3;
    }
    return cluster;
}

// trimit topologia totala de la 1->2->3->0
vector<vector<int>> send_cluster_coord(int rank, vector<vector<int>> cluster) {
    if(rank == 1) {
        for(int i = 0; i < NumberCluster; ++i) {
            int size =  cluster.at(i).size();
            MPI_Send(&size, 1, MPI_INT, 2, 0, MPI_COMM_WORLD);
            MPI_Send(&cluster.at(i)[0], size, MPI_INT, 2, 1, MPI_COMM_WORLD);
            std::cout << "M(" << rank << ",2)\n";
        }
    } else if(rank == 2) {
        for(int i = 0; i < NumberCluster; ++i) {
            int number_work;
            MPI_Recv(&number_work, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, NULL);
            vector<int> workers_recv(number_work);
            MPI_Recv(&workers_recv[0], number_work, MPI_INT, 1, 1, MPI_COMM_WORLD, NULL);
            cluster[workers_recv[0]] = workers_recv;
        }
        for(int i = 0; i < NumberCluster; ++i) {
            int size =  cluster.at(i).size();
            MPI_Send(&size, 1, MPI_INT, 3, 0, MPI_COMM_WORLD);
            MPI_Send(&cluster.at(i)[0], size, MPI_INT, 3, 1, MPI_COMM_WORLD);
            std::cout << "M(" << rank << ",3)\n";
        }
    } else if(rank == 3) {
        for(int i = 0; i < NumberCluster; ++i) {
            int number_work;
            MPI_Recv(&number_work, 1, MPI_INT, 2, 0, MPI_COMM_WORLD, NULL);
            vector<int> workers_recv(number_work);
            MPI_Recv(&workers_recv[0], number_work, MPI_INT, 2, 1, MPI_COMM_WORLD, NULL);
            cluster[workers_recv[0]] = workers_recv;
        }
        for(int i = 0; i < NumberCluster; ++i) {
            int size =  cluster.at(i).size();
            MPI_Send(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&cluster.at(i)[0], size, MPI_INT, 0, 1, MPI_COMM_WORLD);
            std::cout << "M(" << rank << ",0)\n";
        }
    } else if(rank == 0) {
        for(int i = 0; i < NumberCluster; ++i) {
            int number_work;
            MPI_Recv(&number_work, 1, MPI_INT, 3, 0, MPI_COMM_WORLD, NULL);
            vector<int> workers_recv(number_work);
            MPI_Recv(&workers_recv[0], number_work, MPI_INT, 3, 1, MPI_COMM_WORLD, NULL);
            cluster[workers_recv[0]] = workers_recv;
        }
    } 
    return cluster;
}

//trimit topologia la workerii din clusterul corespunzator
void send_cluster(int rank, vector<vector<int>> workers) {
    vector<int> worker = workers[rank];
    for(int i = 0; i < NumberCluster; ++i) {
        for(long unsigned int j = 1; j < worker.size(); ++j) {
            std::cout << "M(" << rank << "," << worker[j] <<")\n";
            int size = workers.at(i).size();
            MPI_Send(&size, 1, MPI_INT, worker[j], 0, MPI_COMM_WORLD);
            MPI_Send(&workers.at(i)[0], size, MPI_INT, worker[j], 1, MPI_COMM_WORLD);
        }
    }
}

//workerii primesc topologia
vector<vector<int>> recv_cluster(int cluster_coord) {
    vector<vector<int>> cluster(NumberCluster);
    for(int i = 0; i < NumberCluster; ++i) {
        int number_work;
        MPI_Recv(&number_work, 1, MPI_INT, cluster_coord, 0, MPI_COMM_WORLD, NULL);
        vector<int> workers_recv(number_work);
        MPI_Recv(&workers_recv[0], number_work, MPI_INT, cluster_coord, 1, MPI_COMM_WORLD, NULL);
        cluster[workers_recv[0]] = workers_recv;
    }
    return cluster;
}

//impart dimeniunea vectorului pentru fiecare cluster in fct de nr de workeri
vector<vector<int>> order_calc(vector<vector<int>> topology, int N, int total_workers) {
    int start, end;
    int vec_size = N;
    int workers_before_me = 0;
    vector<vector<int>> access_vect(4);
    for (int i = 0; i < NumberCluster; i++) {
        start = workers_before_me * vec_size / total_workers;
        workers_before_me += topology[i].size() - 1;
        end = min(workers_before_me * vec_size / total_workers, vec_size);
        vector<int> start_stop = {i, start, end};
        access_vect[i] = start_stop;
    }
    return access_vect;
}

// trimit toate valorile de start si stop la fiecare cluster
void send_access_calc(vector<vector<int>> access_vect,int source, int dest) {
    for (int i = 0; i < NumberCluster; i++) {
        std::cout << "M(" << source << "," << dest <<")\n";
        int size = access_vect.at(i).size();
        MPI_Send(&size, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(&access_vect.at(i)[0], size, MPI_INT, dest, 1, MPI_COMM_WORLD);
    }
}

// primesc toate valorile de start si stop pentru cluster
vector<vector<int>> recv_access_calc(int source) {
    vector<vector<int>> access_vect(NumberCluster);
    for(int i = 0; i < NumberCluster; ++i) {
        int number_work;
        MPI_Recv(&number_work, 1, MPI_INT, source, 0, MPI_COMM_WORLD, NULL);
        vector<int> workers_recv(number_work);
        MPI_Recv(&workers_recv[0], number_work, MPI_INT, source, 1, MPI_COMM_WORLD, NULL);
        access_vect[workers_recv[0]] = workers_recv;
    }
    return access_vect;
}

//impart dimensiunea primita de cluster intre workeri
void send_to_worker(int start, int stop, vector<int> workers) {
    int start_w, end_w;
    int vec_size = stop - start;
    int number_workers = workers.size() - 1;
    for (int i = 0; i < number_workers; i++) {
        start_w = i * vec_size / number_workers + start;
        end_w = min((i + 1) * vec_size / number_workers, vec_size) + start;
        std::cout << "M(" << workers[0] << "," << workers[i + 1] <<")\n";
        MPI_Send(&start_w, 1, MPI_INT, workers[i + 1], 0, MPI_COMM_WORLD);
        MPI_Send(&end_w, 1, MPI_INT, workers[i + 1], 0, MPI_COMM_WORLD);
    }
}

// fiecare worker primeste unde trebuie sa realizeze modificari si trimit vectorul final
void recv_from_cluster(int coord_cluster, int N, vector<int> &calc, int rank) {
    int start, end;
    MPI_Recv(&start, 1, MPI_INT, coord_cluster, 0, MPI_COMM_WORLD, NULL);
    MPI_Recv(&end, 1, MPI_INT, coord_cluster, 0, MPI_COMM_WORLD, NULL);

    for (int i = start; i < end; i++) {
        calc[i] = (N - i - 1) * 5;
    }
    std::cout << "M(" << rank << "," << coord_cluster <<")\n";
    MPI_Send(&calc[0], N, MPI_INT, coord_cluster, 0, MPI_COMM_WORLD);
}

// functia de comparare daca s-au realizat modificari in vectorul primit de cluster
void compar2vector(vector<int> &calc, int source) {
    int N = calc.size();
    vector<int> calc2(N);
    MPI_Recv(&calc2[0], N, MPI_INT, source, 0, MPI_COMM_WORLD, NULL);
    for(int i = 0; i < calc2.size(); i++) {
        if(calc2[i] != calc[i] && calc2[i] != -1) {
            calc[i] = calc2[i];
        }
    }
}

int main(int argc, char *argv[]) {
    int rank, nProcesses, coord_rank;
    int number_worker = 0;
    vector<vector<int>> cluster(NumberCluster);
    vector<vector<int>> access_vect;
    vector<int> workers;
    vector<int> calc;
    MPI_Init(&argc, &argv);
    int N = atoi(argv[1]);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);

    if(rank < 4) {
        workers = send_to_coord(rank);
        cluster[workers[0]] = workers;
        cluster = send_worker(rank, cluster);
        cluster = send_cluster_coord(rank, cluster);
        print_cluster(rank, cluster);
        send_cluster(rank, cluster);
        
        if(rank == 0) {
            for (long unsigned int i = 0; i < cluster.size(); ++i) {
                number_worker += (cluster[i].size() - 1);
            }
            calc = std::vector<int>(N, -1);
            access_vect = order_calc(cluster, N, number_worker);
            send_access_calc(access_vect, 0, 3);
            std::cout << "M(" << rank << ",3)\n";
            MPI_Send(&calc[0], N, MPI_INT, 3, 0, MPI_COMM_WORLD);
            
        } else {
            calc.resize(N);
            if(rank == 3) {
                access_vect = recv_access_calc(0);
                send_access_calc(access_vect, 3, 2);
                MPI_Recv(&calc[0], N, MPI_INT, 0, 0, MPI_COMM_WORLD, NULL);
                std::cout << "M(" << rank << ",2)\n";
                MPI_Send(&calc[0], N, MPI_INT, 2, 0, MPI_COMM_WORLD);
            } else if (rank == 2) {
                access_vect = recv_access_calc(3);
                send_access_calc(access_vect, 2, 1);
                MPI_Recv(&calc[0], N, MPI_INT, 3, 0, MPI_COMM_WORLD, NULL);
                std::cout << "M(" << rank << ",1)\n";
                MPI_Send(&calc[0], N, MPI_INT, 1, 0, MPI_COMM_WORLD);
            } else if (rank == 1) {
                access_vect = recv_access_calc(2);
                MPI_Recv(&calc[0], N, MPI_INT, 2, 0, MPI_COMM_WORLD, NULL);
            }
        }
        for(long unsigned int i = 1; i < workers.size(); ++i) {
            std::cout << "M(" << rank << "," << workers[i] <<")\n";
            MPI_Send(&calc[0], N, MPI_INT, workers[i], 0, MPI_COMM_WORLD);
        }
        send_to_worker(access_vect[rank][1], access_vect[rank][2], workers);
        for(long unsigned int i = 1; i < workers.size(); ++i) {
            vector<int> calc2(N);
            MPI_Recv(&calc2[0], N, MPI_INT, workers[i], 0, MPI_COMM_WORLD, NULL);
            for(int i = 0; i < calc2.size(); i++) {
                if(calc2[i] != calc[i] && calc2[i] != -1) {
                    calc[i] = calc2[i];
                }
            }
        }
        if(rank == 1) {
            std::cout << "M(" << rank << ",2)\n";
            MPI_Send(&calc[0], N, MPI_INT, 2, 0, MPI_COMM_WORLD);
        }
        if(rank == 2) {
            compar2vector(calc, 1);
            std::cout << "M(" << rank << ",3)\n";
            MPI_Send(&calc[0], N, MPI_INT, 3, 0, MPI_COMM_WORLD);
        }
        if(rank == 3) {
            compar2vector(calc, 2);
            std::cout << "M(" << rank << ",0)\n";
            MPI_Send(&calc[0], N, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
        if(rank == 0) {
            compar2vector(calc, 3);
            cout <<"Rezultat:";
            for(int i = 0; i < calc.size(); ++i) {
                cout << " " << calc[i];
            }
            cout << "\n";
        }
        
    } else {
        MPI_Recv(&coord_rank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, NULL);
        cluster = recv_cluster(coord_rank);
        print_cluster(rank, cluster);
        calc.resize(N);
        MPI_Recv(&calc[0], N, MPI_INT, coord_rank, 0, MPI_COMM_WORLD, NULL);
        recv_from_cluster(coord_rank, N, calc, rank);
    }
    
    MPI_Finalize();
}
