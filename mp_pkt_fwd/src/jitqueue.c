/*
 / _____)             _              | |
( (____  _____ ____ _| |_ _____  ____| |__
 \____ \| ___ |    (_   _) ___ |/ ___)  _ \
 _____) ) ____| | | || |_| ____( (___| | | |
(______/|_____)_|_|_| \__)_____)\____)_| |_|
  (C)2013 Semtech-Cycleo

Description:
    LoRa concentrator : Just In Time TX scheduling queue

License: Revised BSD License, see LICENSE.TXT file include in the project
Maintainer: Michael Coracin
*/

/* -------------------------------------------------------------------------- */
/* --- DEPENDANCIES --------------------------------------------------------- */

#define _GNU_SOURCE     /* needed for qsort_r to be defined */
#include <stdlib.h>     /* qsort_r */
#include <stdio.h>      /* printf, fprintf, snprintf, fopen, fputs */
#include <string.h>     /* memset, memcpy */
#include <pthread.h>
#include <assert.h>
#include <math.h>

#include "trace.h"
#include "jitqueue.h"

/* -------------------------------------------------------------------------- */
/* --- PRIVATE MACROS ------------------------------------------------------- */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE CONSTANTS & TYPES -------------------------------------------- */
#define TX_START_DELAY          1500    /* microseconds */
                                        /* TODO: get this value from HAL? */
#define TX_MARGIN_DELAY         1000    /* Packet overlap margin in microseconds */
                                        /* TODO: How much margin should we take? */
#define TX_JIT_DELAY            30000   /* Pre-delay to program packet for TX in microseconds */
#define TX_MAX_ADVANCE_DELAY    ((JIT_NUM_BEACON_IN_QUEUE + 1) * 128 * 1E6) /* Maximum advance delay accepted for a TX packet, compared to current time */

#define BEACON_GUARD            3000000 /* Interval where no ping slot can be placed,
                                            to ensure beacon can be sent */
#define BEACON_RESERVED         2120000 /* Time on air of the beacon, with some margin */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE VARIABLES (GLOBAL) ------------------------------------------- */
static pthread_mutex_t mx_jit_queue = PTHREAD_MUTEX_INITIALIZER; /* control access to JIT queue */

/* -------------------------------------------------------------------------- */
/* --- PRIVATE FUNCTIONS DEFINITION ----------------------------------------- */
static uint32_t time_on_air(struct lgw_pkt_tx_s *packet, bool isBeacon) {
    uint8_t SF, H, DE;
    uint16_t BW;
    uint32_t payloadSymbNb, Tpacket;
    double Tsym, Tpreamble, Tpayload;

    switch (packet->bandwidth) {
        case BW_125KHZ:
            BW = 125;
            break;
        case BW_250KHZ:
            BW = 250;
            break;
        case BW_500KHZ:
            BW = 500;
            break;
        default:
            MSG("ERROR: Cannot compute time on air for this packet, unsupported bandwidth (%u)\n", packet->bandwidth);
            return 0;
    }

    switch (packet->datarate) {
        case DR_LORA_SF7:
            SF = 7;
            break;
        case DR_LORA_SF8:
            SF = 8;
            break;
        case DR_LORA_SF9:
            SF = 9;
            break;
        case DR_LORA_SF10:
            SF = 10;
            break;
        case DR_LORA_SF11:
            SF = 11;
            break;
        case DR_LORA_SF12:
            SF = 12;
            break;
        default:
            MSG("ERROR: Cannot compute time on air for this packet, unsupported datarate (%u)\n", packet->datarate);
            return 0;
    }

    /* Duration of 1 symbol */
    Tsym = (double)(1 << SF) / BW;

    /* Duration of preamble */
    Tpreamble = (8 + 4.25) * Tsym; /* 8 programmed symbols in preamble */

    /* Duration of payload */
    H = (isBeacon==false)?0:1; /* header is always enabled, except for beacons */
    DE = (SF >= 11)?1:0; /* Low datarate optimization enabled for SF11 and SF12 */

    payloadSymbNb = 8 + (ceil((double)(8*packet->size - 4*SF + 28 + 16 - 20*H) / (double)(4*(SF - 2*DE))) * (packet->coderate + 4)); /* Explicitely cast to double to keep precision of the division */

    Tpayload = payloadSymbNb * Tsym;

    Tpacket = Tpreamble + Tpayload;

    return Tpacket;
}

/* -------------------------------------------------------------------------- */
/* --- PUBLIC FUNCTIONS DEFINITION ----------------------------------------- */

bool jit_queue_is_full(struct jit_queue_s *queue) {
    bool result;

    pthread_mutex_lock(&mx_jit_queue);

    result = (queue->num_pkt == JIT_QUEUE_MAX)?true:false;

    pthread_mutex_unlock(&mx_jit_queue);

    return result;
}

bool jit_queue_is_empty(struct jit_queue_s *queue) {
    bool result;

    pthread_mutex_lock(&mx_jit_queue);

    result = (queue->num_pkt == 0)?true:false;

    pthread_mutex_unlock(&mx_jit_queue);

    return result;
}

void jit_queue_init(struct jit_queue_s *queue) {
    int i;

    pthread_mutex_lock(&mx_jit_queue);

    memset(queue, 0, sizeof(*queue));
    for (i=0; i<JIT_QUEUE_MAX; i++) {
        queue->nodes[i].pre_delay = 0;
        queue->nodes[i].post_delay = 0;
    }

    pthread_mutex_unlock(&mx_jit_queue);
}

#ifdef __MACH__
  int compare(void *arg, const void *a, const void *b)
#else
  int compare(const void *a, const void *b, void *arg)
#endif
{
    struct jit_node_s *p = (struct jit_node_s *)a;
    struct jit_node_s *q = (struct jit_node_s *)b;
    int *counter = (int *)arg;
    int p_count, q_count;

    p_count = p->pkt.count_us;
    q_count = q->pkt.count_us;

    if (p_count > q_count)
        *counter = *counter + 1;

    return p_count - q_count;
}

void jit_sort_queue(struct jit_queue_s *queue) {
    int counter = 0;

    if (queue->num_pkt == 0) {
        return;
    }

    MSG_DEBUG(DEBUG_JIT, "sorting queue in ascending order packet timestamp - queue size:%u\n", queue->num_pkt);
#ifdef __MACH__
    qsort_r(queue->nodes, queue->num_pkt, sizeof(queue->nodes[0]), &counter, compare);
#else
    qsort_r(queue->nodes, queue->num_pkt, sizeof(queue->nodes[0]), compare, &counter);
#endif
    MSG_DEBUG(DEBUG_JIT, "sorting queue done - swapped:%d\n", counter);
}

bool jit_collision_test(uint32_t p1_count_us, uint32_t p1_pre_delay, uint32_t p1_post_delay, uint32_t p2_count_us, uint32_t p2_pre_delay, uint32_t p2_post_delay) {
    if (((p1_count_us - p2_count_us) <= (p1_pre_delay + p2_post_delay + TX_MARGIN_DELAY)) ||
        ((p2_count_us - p1_count_us) <= (p2_pre_delay + p1_post_delay + TX_MARGIN_DELAY))) {
        return true;
    } else {
        return false;
    }
}

/* non mutex protected dequeue action, BE AWARE to protect via mx_jit_queue from the caller side */
void jit_dequeue_action(struct jit_queue_s *queue, int index, struct lgw_pkt_tx_s *packet,enum jit_pkt_type_e *pkt_type){
    if(packet!=NULL){//if null the caller does not need the dequeued package
	/* Dequeue requested packet */
	memcpy(packet, &(queue->nodes[index].pkt), sizeof(struct lgw_pkt_tx_s));
    }
    queue->num_pkt--;
    if( pkt_type !=NULL ) *pkt_type = queue->nodes[index].pkt_type;
    if ((queue->nodes[index].pkt_type == JIT_PKT_TYPE_BEACON)||(queue->nodes[index].pkt_type == JIT_PKT_TYPE_BEACON_SIM)) {
	queue->num_beacon--;
    }
    /* Replace dequeued packet with last packet of the queue */
    memcpy(&(queue->nodes[index]), &(queue->nodes[queue->num_pkt]), sizeof(struct jit_node_s));
    memset(&(queue->nodes[queue->num_pkt]), 0, sizeof(struct jit_node_s));

    /* Sort queue in ascending order of packet timestamp */
    jit_sort_queue(queue);

    if(packet!=NULL){
	MSG_DEBUG(DEBUG_JIT, "dequeued packet with count_us=%u from index %d\n", packet->count_us, index);
    }else{
	MSG_DEBUG(DEBUG_JIT, "dequeued packet from index %d\n", index);
    }
}

enum jit_error_e jit_enqueue(struct jit_queue_s *queue, struct timeval *time, struct lgw_pkt_tx_s *packet, enum jit_pkt_type_e pkt_type, bool prio_package) {
    int i = 0;
    uint32_t time_us = time->tv_sec * 1000000UL + time->tv_usec; /* convert time in µs */
    uint32_t packet_post_delay = 0;
    uint32_t packet_pre_delay = 0;
    uint32_t target_pre_delay = 0;
    enum jit_error_e err_collision;
    uint32_t asap_count_us;
    uint8_t last_prio_package_index=0;
    uint8_t delete_counter=0;
    uint8_t last_low_prio_package_index=0;

    MSG_DEBUG(DEBUG_JIT, "Current concentrator time is %u, pkt_type=%d, prio=%u\n", time_us, pkt_type,prio_package);

    if (packet == NULL) {
        MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: invalid parameter\n");
        return JIT_ERROR_INVALID;
    }
    if (jit_queue_is_full(queue)&&(prio_package==false)) {//only return for a full queue if there is no possibility to drop lower prio packages
        MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: cannot enqueue packet, JIT queue is full\n");
        return JIT_ERROR_FULL;
    }

    /* Compute packet pre/post delays depending on packet's type */
    switch (pkt_type) {
        case JIT_PKT_TYPE_DOWNLINK_CLASS_A:
        case JIT_PKT_TYPE_DOWNLINK_CLASS_B:
        case JIT_PKT_TYPE_DOWNLINK_CLASS_C:
        case JIT_PKT_TYPE_DOWNLINK_CLASS_A_SIM:
        case JIT_PKT_TYPE_DOWNLINK_CLASS_B_SIM:
        case JIT_PKT_TYPE_DOWNLINK_CLASS_C_SIM:
            packet_pre_delay = TX_START_DELAY + TX_JIT_DELAY;
            packet_post_delay = time_on_air(packet, false) * 1000UL; /* in us */
            break;
        case JIT_PKT_TYPE_BEACON:
        case JIT_PKT_TYPE_BEACON_SIM:
            /* As defined in LoRaWAN spec */
            packet_pre_delay = TX_START_DELAY + BEACON_GUARD + TX_JIT_DELAY;
            packet_post_delay = BEACON_RESERVED;
            break;
        default:
            break;
    }

    pthread_mutex_lock(&mx_jit_queue);

    //full check for high prio packages, if there are only high prio packages in the queue no need to go on
    for (i=0; i<queue->num_pkt; i++) {
	if(queue->nodes[i].prio_package==false) break;
    }
    if(i==JIT_QUEUE_MAX){
        MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: cannot enqueue high prio packet, JIT queue is full with only high prio packages in it\n");
	pthread_mutex_unlock(&mx_jit_queue);
    	return JIT_ERROR_FULL;
    }

    /* An immediate downlink becomes a timestamped downlink "ASAP" */
    /* Set the packet count_us to the first available slot */
    if ((pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_C)||(pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_C_SIM)) {
        /* change tx_mode to timestamped */
        packet->tx_mode = TIMESTAMPED;

        /* Search for the ASAP timestamp to be given to the packet */
        asap_count_us = time_us + 1E6; /* TODO: Take 1 second margin, to be refined */
        if (queue->num_pkt == 0) {
            /* If the jit queue is empty, we can insert this packet */
            MSG_DEBUG(DEBUG_JIT, "DEBUG: insert IMMEDIATE downlink, first in JiT queue (count_us=%u)\n", asap_count_us);
        } else {
            /* Else we can try to insert it:
                - ASAP meaning NOW + MARGIN
                - at the last index of the queue
                - between 2 downlinks in the queue
            */

    	    /* First, try if the ASAP time collides with an already enqueued downlink */
    	    for (i=0; i<queue->num_pkt; i++) {
            	if (jit_collision_test(asap_count_us, packet_pre_delay, packet_post_delay, queue->nodes[i].pkt.count_us, queue->nodes[i].pre_delay, queue->nodes[i].post_delay) == true) {
			if((prio_package==true)&&(queue->nodes[i].prio_package==false)){//for a high prio package skip low prio checks in queue as they will/could be removed later anyways
			    continue;
			}
                	MSG_DEBUG(DEBUG_JIT, "DEBUG: cannot insert IMMEDIATE downlink at count_us=%u, collides with %u (index=%d)\n", asap_count_us, queue->nodes[i].pkt.count_us, i);
                	break;
		}
    	    }
    	    if (i == queue->num_pkt) {
            	/* No collision (or removeable ones) with ASAP time, we can insert it */
            	MSG_DEBUG(DEBUG_JIT, "DEBUG: insert IMMEDIATE downlink ASAP at %u (no collision)\n", asap_count_us);
    	    } else {
            	/* Search for the best slot then as the ASAP time was not collision free for high and low prio packages */
            	for (i=0; i<queue->num_pkt; i++) {
		    asap_count_us = queue->nodes[i].pkt.count_us + queue->nodes[i].post_delay + packet_pre_delay + TX_JIT_DELAY + TX_MARGIN_DELAY;
            	    if (i == (queue->num_pkt - 1)) {
			if(prio_package==false){
                    	    /* Last packet index, we can insert after this one */
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: insert IMMEDIATE downlink, last in JiT queue (count_us=%u)\n", asap_count_us);
			}else{//for high prio packages, place the new package right after the last detected high prio package
			    if(queue->nodes[i].prio_package==true) last_prio_package_index=i;//if the last element is also a high prio item, place the new package timely right afterwords
			    //in all other cases place it after the last seen high prio package, consequently removing the followng low prio package in the later loop
			    asap_count_us = queue->nodes[last_prio_package_index].pkt.count_us + queue->nodes[last_prio_package_index].post_delay + packet_pre_delay + TX_JIT_DELAY + TX_MARGIN_DELAY;
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: insert IMMEDIATE downlink, after last prio package in JiT queue (count_us=%u)\n", asap_count_us);
			}
            	     } else {
			if(prio_package==true){
			    if(queue->nodes[i].prio_package==true){
				last_prio_package_index=i;//remember the last high prio queue item
			    }
			    //insertion time is always right after the last seen high prio package in queue
			    asap_count_us = queue->nodes[last_prio_package_index].pkt.count_us + queue->nodes[last_prio_package_index].post_delay + packet_pre_delay + TX_JIT_DELAY + TX_MARGIN_DELAY;
			    if(queue->nodes[i+1].prio_package==false){
				continue;//skip all collision checks for all low prio queue items until the next(=i+1) package is high prio
			    }
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: try to insert IMMEDIATE downlink (count_us=%u) between index %d and index %d?\n", asap_count_us, last_prio_package_index, i+1);
			}else{
		    	    /* Check if packet can be inserted between this index and the next one */
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: try to insert IMMEDIATE downlink (count_us=%u) between index %d and index %d?\n", asap_count_us, i, i+1);
			}
                    	if (jit_collision_test(asap_count_us, packet_pre_delay, packet_post_delay, queue->nodes[i+1].pkt.count_us, queue->nodes[i+1].pre_delay, queue->nodes[i+1].post_delay) == true) {
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: failed to insert IMMEDIATE downlink (count_us=%u), continue...\n", asap_count_us);
                    	    continue;
                    	} else {
                    	    MSG_DEBUG(DEBUG_JIT, "DEBUG: insert IMMEDIATE downlink (count_us=%u)\n", asap_count_us);
                    	    break;
                    	}
            	    }
        	}
	    }
	}
        /* Set packet with ASAP timestamp */
    	packet->count_us = asap_count_us;
    }

    /* Check criteria_1: is it already too late to send this packet ?
     *  The packet should arrive at least at (tmst - TX_START_DELAY) to be programmed into concentrator
     *  Note: - Also add some margin, to be checked how much is needed, if needed
     *        - Valid for both Downlinks and Beacon packets
     *
     *  Warning: unsigned arithmetic (handle roll-over)
     *      t_packet < t_current + TX_START_DELAY + MARGIN
     */
    if ((packet->count_us - time_us) <= (TX_START_DELAY + TX_MARGIN_DELAY + TX_JIT_DELAY) ||
        (time_us - packet->count_us) < 1E6) {
        MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: Packet REJECTED, already too late to send it (current=%u, packet=%u, type=%d)\n", time_us, packet->count_us, pkt_type);
        pthread_mutex_unlock(&mx_jit_queue);
        return JIT_ERROR_TOO_LATE;
    }

    /* Check criteria_2: Does packet timestamp seem plausible compared to current time
     *  We do not expect the server to program a downlink too early compared to current time
     *  Class A: downlink has to be sent in a 1s or 2s time window after RX
     *  Class B: downlink has to occur in a 128s time window
     *  Class C: no check needed, departure time has been calculated previously
     *  So let's define a safe delay above which we can say that the packet is out of bound: TX_MAX_ADVANCE_DELAY
     *  Note: - Valid for Downlinks only, not for Beacon packets
     *
     *  Warning: unsigned arithmetic (handle roll-over)
                t_packet > t_current + TX_MAX_ADVANCE_DELAY
     */
    if ((pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_A) || (pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_B)||(pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_A_SIM) || (pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_B_SIM)) {
        if ((packet->count_us - time_us) > TX_MAX_ADVANCE_DELAY) {
            MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: Packet REJECTED, timestamp seems wrong, too much in advance (current=%u, packet=%u, type=%d)\n", time_us, packet->count_us, pkt_type);
            pthread_mutex_unlock(&mx_jit_queue);
            return JIT_ERROR_TOO_EARLY;
        }
    }


    /* Check criteria_3: does this new packet overlap with a packet already enqueued ?
     *  Note: - need to take into account packet's pre_delay and post_delay of each packet
     *        - Valid for both Downlinks and beacon packets
     *        - Beacon guard can be ignored if we try to queue a Class A downlink
     */
    for (i=0; i<queue->num_pkt; i++) {
        /* We ignore Beacon Guard for Class A/C downlinks */
        if (((pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_A) || (pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_C)||(pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_A_SIM) || (pkt_type == JIT_PKT_TYPE_DOWNLINK_CLASS_C_SIM)) && ((queue->nodes[i].pkt_type == JIT_PKT_TYPE_BEACON)||(queue->nodes[i].pkt_type == JIT_PKT_TYPE_BEACON_SIM))) {
            target_pre_delay = TX_START_DELAY;
        } else {
            target_pre_delay = queue->nodes[i].pre_delay;
        }

        /* Check if there is a collision
         *  Warning: unsigned arithmetic (handle roll-over)
         *      t_packet_new - pre_delay_packet_new < t_packet_prev + post_delay_packet_prev (OVERLAP on post delay)
         *      t_packet_new + post_delay_packet_new > t_packet_prev - pre_delay_packet_prev (OVERLAP on pre delay)
         */
	if(queue->nodes[i].prio_package==false) last_low_prio_package_index=i;
        if (jit_collision_test(packet->count_us, packet_pre_delay, packet_post_delay, queue->nodes[i].pkt.count_us, target_pre_delay, queue->nodes[i].post_delay) == true) {
	    if((prio_package==true) && (queue->nodes[i].prio_package==false)){
		//mark the package here for deletion, as we do not know, if we will find a collision free solution at all
		queue->nodes[i].delete_package=true;
		delete_counter++;
		continue;
	    }
            switch (queue->nodes[i].pkt_type) {
                case JIT_PKT_TYPE_DOWNLINK_CLASS_A:
                case JIT_PKT_TYPE_DOWNLINK_CLASS_B:
                case JIT_PKT_TYPE_DOWNLINK_CLASS_C:
                case JIT_PKT_TYPE_DOWNLINK_CLASS_A_SIM:
                case JIT_PKT_TYPE_DOWNLINK_CLASS_B_SIM:
                case JIT_PKT_TYPE_DOWNLINK_CLASS_C_SIM:
                    MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: Packet (type=%d) REJECTED, collision with packet already programmed at %u (%u)\n", pkt_type, queue->nodes[i].pkt.count_us, packet->count_us);
                    err_collision = JIT_ERROR_COLLISION_PACKET;
                    break;
                case JIT_PKT_TYPE_BEACON:
                case JIT_PKT_TYPE_BEACON_SIM:
                    if ((pkt_type != JIT_PKT_TYPE_BEACON)&&(pkt_type != JIT_PKT_TYPE_BEACON_SIM)) {
                        /* do not overload logs for beacon/beacon collision, as it is expected to happen with beacon pre-scheduling algorith used */
                        MSG_DEBUG(DEBUG_JIT_ERROR, "ERROR: Packet (type=%d) REJECTED, collision with beacon already programmed at %u (%u)\n", pkt_type, queue->nodes[i].pkt.count_us, packet->count_us);
                    }
                    err_collision = JIT_ERROR_COLLISION_BEACON;
                    break;
                default:
                    MSG("ERROR: Unknown packet type, should not occur, BUG?\n");
                    assert(0);
                    break;
            }
	    for (i=0; i<queue->num_pkt; i++){//reset deletion as we could not find any collision free time frame
		queue->nodes[i].delete_package=false;
	    }
            pthread_mutex_unlock(&mx_jit_queue);
            return err_collision;
        }
    }
    if((delete_counter==0)&&(queue->num_pkt==JIT_QUEUE_MAX)){//special case no conflicts, but also no space left in queue, this can here only happen if we can delete at least one low prio package
	delete_counter=1;queue->nodes[last_low_prio_package_index].delete_package=true;
        MSG_DEBUG(DEBUG_JIT, "JIT full, THROWING OUT out last low prio package\n");
	if(queue->nodes[last_low_prio_package_index].prio_package){//that is a bug
            MSG("ERROR: Would have dropped a high prio package this is a bug\n");
	    assert(0);
	}
    }
    if(delete_counter!=0){
	//dequeue all marked packages
	for (i=0; i<queue->num_pkt; i++) {
	    if(queue->nodes[i].delete_package==true){
        	MSG_DEBUG(DEBUG_JIT, "THROWING OUT low prio packet at %u index %u length %u (for packet with timestamp %u and length %u)\n", queue->nodes[i].pkt.count_us,i,queue->nodes[i].post_delay+target_pre_delay,packet->count_us,packet_pre_delay+packet_post_delay);
		if(queue->nodes[i].prio_package){//that is a bug
            	    MSG("ERROR: Would have dropped a high prio package this is a bug\n");
		    assert(0);
		}
		jit_dequeue_action(queue,i,NULL,NULL);
		i=0;
	    }
	}
    }
    /* Finally enqueue it */
    /* Insert packet at the end of the queue */
    memcpy(&(queue->nodes[queue->num_pkt].pkt), packet, sizeof(struct lgw_pkt_tx_s));
    queue->nodes[queue->num_pkt].pre_delay = packet_pre_delay;
    queue->nodes[queue->num_pkt].post_delay = packet_post_delay;
    queue->nodes[queue->num_pkt].pkt_type = pkt_type;
    queue->nodes[queue->num_pkt].prio_package = prio_package;
    if ((pkt_type == JIT_PKT_TYPE_BEACON)||(pkt_type == JIT_PKT_TYPE_BEACON_SIM)) {
        queue->num_beacon++;
    }
    queue->num_pkt++;
    /* Sort the queue in ascending order of packet timestamp */
    jit_sort_queue(queue);

    /* Done */
    pthread_mutex_unlock(&mx_jit_queue);

    jit_print_queue(queue, false, DEBUG_JIT);

    MSG_DEBUG(DEBUG_JIT, "enqueued packet with count_us=%u (size=%u bytes, toa=%u us, type=%u)\n", packet->count_us, packet->size, packet_post_delay, pkt_type);

    return JIT_ERROR_OK;
}

enum jit_error_e jit_peek_and_dequeue(struct jit_queue_s *queue, struct timeval *time, struct lgw_pkt_tx_s *packet, enum jit_pkt_type_e *pkt_type) {
    /* Return index of node containing a packet inline with given time */
    int i = 0;
    int idx_highest_priority = -1;
    uint32_t time_us;

    if ((time == NULL)||(packet==NULL)) {
        MSG("ERROR: invalid parameter\n");
        return JIT_ERROR_INVALID;
    }

    if (jit_queue_is_empty(queue)) {
        return JIT_ERROR_EMPTY;
    }

    time_us = time->tv_sec * 1000000UL + time->tv_usec;

    pthread_mutex_lock(&mx_jit_queue);

    /* Search for highest priority packet to be sent */
    for (i=0; i<queue->num_pkt; i++) {
        /* First check if that packet is outdated:
         *  If a packet seems too much in advance, and was not rejected at enqueue time,
         *  it means that we missed it for peeking, we need to drop it
         *
         *  Warning: unsigned arithmetic
         *      t_packet > t_current + TX_MAX_ADVANCE_DELAY
         */
        if ((queue->nodes[i].pkt.count_us - time_us) >= TX_MAX_ADVANCE_DELAY) {
            /* We drop the packet to avoid lock-up */
            queue->num_pkt--;
            if ((queue->nodes[i].pkt_type == JIT_PKT_TYPE_BEACON)||(queue->nodes[i].pkt_type == JIT_PKT_TYPE_BEACON_SIM)) {
                queue->num_beacon--;
                MSG("WARNING: --- Beacon dropped (current_time=%u, packet_time=%u) ---\n", time_us, queue->nodes[i].pkt.count_us);
            } else {
                MSG("WARNING: --- Packet dropped (current_time=%u, packet_time=%u) ---\n", time_us, queue->nodes[i].pkt.count_us);
            }

            /* Replace dropped packet with last packet of the queue */
            memcpy(&(queue->nodes[i]), &(queue->nodes[queue->num_pkt]), sizeof(struct jit_node_s));
            memset(&(queue->nodes[queue->num_pkt]), 0, sizeof(struct jit_node_s));

            /* Sort queue in ascending order of packet timestamp */
            jit_sort_queue(queue);

            /* restart loop  after purge to find packet to be sent */
            i = 0;
            continue;
        }

        /* Then look for highest priority packet to be sent:
         *  Warning: unsigned arithmetic (handle roll-over)
         *      t_packet < t_highest
         */
        if ((idx_highest_priority == -1) || (((queue->nodes[i].pkt.count_us - time_us) < (queue->nodes[idx_highest_priority].pkt.count_us - time_us)))) {
            idx_highest_priority = i;
        }
    }

    /* Peek criteria 1: look for a packet to be sent in next TX_JIT_DELAY ms timeframe
     *  Warning: unsigned arithmetic (handle roll-over)
     *      t_packet < t_current + TX_JIT_DELAY
     */
    if ((queue->nodes[idx_highest_priority].pkt.count_us - time_us) < TX_JIT_DELAY) {
        MSG_DEBUG(DEBUG_JIT, "peek and dequeue packet with count_us=%u at index %d\n",
            queue->nodes[idx_highest_priority].pkt.count_us, idx_highest_priority);

	jit_dequeue_action(queue,idx_highest_priority,packet,pkt_type);
	pthread_mutex_unlock(&mx_jit_queue);
	jit_print_queue(queue, false, DEBUG_JIT);
	return JIT_ERROR_OK;
    }

    pthread_mutex_unlock(&mx_jit_queue);
    return JIT_ERROR_EMPTY;
}

void jit_print_queue(struct jit_queue_s *queue, bool show_all, int debug_level) {
    int i = 0;
    int loop_end;

    if (jit_queue_is_empty(queue)) {
        MSG_DEBUG(debug_level, "INFO: [jit] queue is empty\n");
    } else {
        pthread_mutex_lock(&mx_jit_queue);

        MSG_DEBUG(debug_level, "INFO: [jit] queue contains %d packets:\n", queue->num_pkt);
        MSG_DEBUG(debug_level, "INFO: [jit] queue contains %d beacons:\n", queue->num_beacon);
        loop_end = (show_all == true) ? JIT_QUEUE_MAX : queue->num_pkt;
        for (i=0; i<loop_end; i++) {
            MSG_DEBUG(debug_level, " - node[%d]: count_us=%u - type=%d prio:%u\n",
                        i,
                        queue->nodes[i].pkt.count_us,
                        queue->nodes[i].pkt_type,queue->nodes[i].prio_package);
        }

        pthread_mutex_unlock(&mx_jit_queue);
    }
}

void jit_report_queue(struct jit_queue_s *queue) {
	uint8_t num_pkt, num_beacon;
    pthread_mutex_unlock(&mx_jit_queue);
    num_pkt    = queue->num_pkt;
    num_beacon = queue->num_beacon;
    pthread_mutex_unlock(&mx_jit_queue);
    printf("# INFO: JIT queue contains %d packets.\n", num_pkt);
    printf("# INFO: JIT queue contains %d beacons.\n", num_beacon);
}

char *jit_error(enum jit_error_e error) {
    switch (error) {
        case JIT_ERROR_OK: return "Packet ok to be sent";
        case JIT_ERROR_TOO_LATE: return "Too late to send this packet";
        case JIT_ERROR_TOO_EARLY: return "Too early to queue this packet";
        case JIT_ERROR_FULL: return "Downlink queue is full";
        case JIT_ERROR_EMPTY: return "Downlink queue is empty";
        case JIT_ERROR_COLLISION_PACKET: return "A packet is already enqueued for this timeframe";
        case JIT_ERROR_COLLISION_BEACON: return "A beacon is planned for this timeframe";
        case JIT_ERROR_TX_FREQ: return "The required frequency for downlink is not supported";
        case JIT_ERROR_TX_POWER: return "The required power for downlink is not supported";
        case JIT_ERROR_GPS_UNLOCKED: return "GPS timestamp could not be used as GPS is unlocked";
        case JIT_ERROR_INVALID: return "Packet is invalid";
	default: return "Invalid error code";
    }
}
