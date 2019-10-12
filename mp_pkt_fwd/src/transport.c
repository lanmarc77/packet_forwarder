/*
 * transport.c
 *
 *  Created on: Feb 10, 2017
 *      Author: Jac Kersing
 */

#include <stdint.h>         /* C99 types */
#include <stdbool.h>        /* bool type */
#include <stdio.h>          /* printf, fprintf, snprintf, fopen, fputs */

#include <string.h>         /* memset */
#include <signal.h>         /* sigaction */
#include <time.h>           /* time, clock_gettime, strftime, gmtime */
#include <sys/time.h>       /* timeval */
#include <unistd.h>         /* getopt, access */
#include <stdlib.h>         /* atoi, exit */
#include <errno.h>          /* error messages */
#include <math.h>           /* modf */
#include <assert.h>

#include <pthread.h>
#include <semaphore.h>

#include "mp_pkt_fwd.h"
#include "trace.h"
#include "loragw_hal.h"
#include "connector.h"
#include "transport.h"
#include "semtech_transport.h"
#include "ttn_transport.h"
#include "gwtraf_transport.h"

extern int serv_count;
extern Server servers[];

void transport_init();
void transport_start();
void transport_stop();
void transport_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt, bool send_report);

// Initialize all data structures
void transport_init() {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		memset(&servers[i], 1, sizeof(Server));
		servers[i].type = semtech;
		servers[i].enabled = false;
		servers[i].upstream = true;
		servers[i].downstream = true;
		servers[i].statusstream = true;
		servers[i].live = false;
		servers[i].connecting = false;
		servers[i].critical = true;
		servers[i].sock_up = -1;
		servers[i].sock_down = -1;
		servers[i].queue = NULL;
		servers[i].serv_filter = 0;
		servers[i].priority = false;
		sem_init(&servers[i].send_sem, 0, 0);
	}
}

void transport_start() {
	int i;
	MSG("INFO: [Transports] Initializing protocol for %d servers\n",serv_count);
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true) {
			switch (servers[i].type) {
				case semtech: 
					semtech_init(i);
					break;
				case ttn_gw_bridge:
					ttn_init(i);
					break;
				case gwtraf:
					gwtraf_init(i);
					break;
			}
		}
	}
}

void transport_stop() {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true) {
			switch (servers[i].type) {
				case semtech:
					semtech_stop(i);
					break;
				case ttn_gw_bridge:
					ttn_stop(i);
					break;
				case gwtraf:
					gwtraf_stop(i);
					break;
			}
		}
	}
}

/*
filter: the configuration value for the server filter from the json file, bit oriented:
    0: if set, all (uplink) packages with net id 00/01 received by the gateway are not forwarded to the server
    1: if set, all (uplink) packages with a different net id than 00/01 received by the gateway are not forwarded to the server
    2: if set, all (downlink) packages received by this server with net id 00/01 are not send to the gateway
    3: if set, all (downlink) packages received by this server with net id different than 00/01 are not send to the gateway
package_type: the lorawan package type
packagae_netid: the first octet of the network id
uplink: true if uplink package (received by gw to be transmitted to server), false otherwise (received by server to be transmitted to gw)

return: true if the package is not supposed to be sent
*/
bool transport_filter(char filter,unsigned char package_type,unsigned char package_netid,bool uplink){
    bool retValue=false;
    package_type=((package_type)>>5)&0x07;
    if((filter!=0)&&(package_type>=2)&&(package_type<=5)){//only filter data packages
        if(uplink==true){
            if(filter&1){
                if((package_netid==00)||(package_netid==01)){
                    retValue=true;
                }
            }
            if(filter&2){
                if((package_netid!=00)&&(package_netid!=01)){
                    retValue=true;
                }
            }
        }else{
            if(filter&4){
                if((package_netid==00)||(package_netid==01)){
                    retValue=true;
                }
            }
            if(filter&8){
                if((package_netid!=00)&&(package_netid!=01)){
                    retValue=true;
                }
            }
        }
    }
    return retValue;
}

void transport_data_up(int nb_pkt, struct lgw_pkt_rx_s *rxpkt, bool send_report) {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true && servers[i].upstream == true) {
			if(transport_filter(servers[i].serv_filter,rxpkt->payload[0],rxpkt->payload[4],true)==false){
				switch (servers[i].type) {
					case semtech:
						semtech_data_up(i, nb_pkt, rxpkt, send_report);
						break;
					case ttn_gw_bridge:
						ttn_data_up(i, nb_pkt, rxpkt);
						break;
					case gwtraf:
						gwtraf_data_up(i, nb_pkt, rxpkt);
						break;
				}
			}
		}
	}
}

void transport_status_up(uint32_t rx_rcv,uint32_t rx_ok,uint32_t tx_tot,uint32_t tx_ok) {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true && servers[i].type == ttn_gw_bridge && servers[i].statusstream == true) {
			ttn_status_up(i, rx_rcv, rx_ok, tx_tot, tx_ok);
		}
	}
}

void transport_status() {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true && servers[i].type == ttn_gw_bridge) {
			ttn_status(i);
		}
	}
}

void transport_send_downtraf(char *json, int len) {
	int i;
	for (i = 0; i < MAX_SERVERS; i++) {
		if (servers[i].enabled == true && servers[i].type == gwtraf) {
			gwtraf_downtraf(i, json, len);
		}
	}
}

// vi: ts=4 sw=4

