#include "channel.h"

#define UNBUFFERED 1
#define BUFFERED 0
#define UNBUFFERED_SEND 0
#define UNBUFFERED_RECEIVE 1
#define NO_UNBUFFERED_OPERATION -1

// Creates a new channel with the provided size and returns it to the caller
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */

    channel_t* channel = (channel_t*) malloc(sizeof(channel_t));
    
    pthread_mutex_init(&channel->mutex, NULL);
    pthread_mutex_init(&channel->select_mutex, NULL);
    pthread_cond_init(&channel->cond_full, NULL);
    pthread_cond_init(&channel->cond_empty, NULL);
    pthread_cond_init(&channel->cond_waiting_stage, NULL);
    pthread_cond_init(&channel->cond_completed_stage, NULL);

    channel->is_closed = false;
    channel->semaphore_select_list_send = list_create();
    channel->semaphore_select_list_recv = list_create();
    channel->unbuffered_operation = NO_UNBUFFERED_OPERATION;
    channel->unbuffered_stage = 0;
    channel->unbuffered = (size == 0) ? UNBUFFERED : BUFFERED;
    if (!channel->unbuffered){
        channel->buffer = buffer_create(size);
    }
    channel->data = NULL;

    channel->send_waiting = 0;
    channel->recv_waiting = 0;

    return channel;
}

// Signal all the semaphores in the select list with only send operations
// This function is called whenever receive operation is successful
// This function is also called when the channel is closed
void signal_semaphore_select_send(channel_t* channel)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_node_t* node = list_head(channel->semaphore_select_list_send);

    while (node != NULL)
    {
        sem_post((sem_t*)node->data);
        node = node->next;
    }
        
    pthread_mutex_unlock(&channel->select_mutex);
}

// Signal all the semaphores in the select list with only receieve operations
// This function is called whenever send operation is successful
// This function is also called when the channel is closed
void signal_semaphore_select_recv(channel_t* channel)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_node_t* node = list_head(channel->semaphore_select_list_recv);

    while (node != NULL)
    {
        sem_post((sem_t*)node->data);
        node = node->next;
    }
        
    pthread_mutex_unlock(&channel->select_mutex);
}

// synchronize the unbuffered operation between a send and a receive operation
// This function is called by channel_send and channel_receive
// This function is also called by channel_non_blocking_send and channel_non_blocking_receive but only when there is an opposite operation waiting in stage 1
// or when there is an opposite operation semaphore waiting in select list
enum channel_status unbuffered_sync(channel_t* channel, int operation, void** data)
{
    stage0:
    
    // stage 1: the first stage of the unbuffered operation where the operation is initiated
    if (channel->unbuffered_stage == 0)
    {

        /* IMPLEMENT THIS */
        channel->unbuffered_stage = 1;
        channel->unbuffered_operation = operation;

        //this data is used to store the data to be sent or received in the second stage of the unbuffered operation
        channel->data = data;

        // signal the semaphore of the opposite operation in select list that the operation is initiated
        if (operation == UNBUFFERED_SEND)
        {
            signal_semaphore_select_recv(channel);
            pthread_cond_broadcast(&channel->cond_empty);
        }
        else if (operation == UNBUFFERED_RECEIVE)
        {
            signal_semaphore_select_send(channel);
            pthread_cond_broadcast(&channel->cond_full);
        }

        //this condition is just used to block the thread until the second stage operation is completed    
        pthread_cond_wait(&channel->cond_completed_stage, &channel->mutex); 

        channel->unbuffered_stage = 0;

        // signal the operations waiting in stage 2 that the operation is completed and they can proceed with stage 1 again
        pthread_cond_broadcast(&channel->cond_waiting_stage);

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        
        return SUCCESS;

    }
    // stage 2: the second stage of the unbuffered operation where the operation is completed when one operation is already waiting to be completed
    else if(channel->unbuffered_stage == 1)
    {   
        /* IMPLEMENT THIS */

        // if the operation is the same as the operation that is already waiting to be completed that means it has to wait
        if(channel->unbuffered_operation == operation)
        {

            if(operation == UNBUFFERED_SEND)
            {
                channel->send_waiting++;
            }
            else if(operation == UNBUFFERED_RECEIVE)
            {
                channel->recv_waiting++;
            }

            // this condition is just used to block the thread until the compatible first stage operation is completed
            pthread_cond_wait(&channel->cond_waiting_stage, &channel->mutex);

            if(operation == UNBUFFERED_SEND)
            {
                channel->send_waiting--;
            }
            else if(operation == UNBUFFERED_RECEIVE)
            {
                channel->recv_waiting--;
            }

            // go to stage 0 again
            goto stage0;
        }

        // if the operation is different from the operation that is already waiting to be completed that means it has to complete the operation
        if (channel->unbuffered_operation != operation)
        {
            if (operation == UNBUFFERED_RECEIVE)
            {
                *data = *channel->data;

            }
            else if (operation == UNBUFFERED_SEND)
            {   
                *channel->data = *data;
            }
        }

        // this is a preventive measure to avoid any operation interfering with stage 1 or 2 while other operation is still in progress
        // this will allow any interfering operation to go directly to wait stage 3 if it interfered between cond_full is signaled and stage 1 grabs the locks again
        channel->unbuffered_stage = 2;
        pthread_cond_signal(&channel->cond_completed_stage);

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }

        return SUCCESS;
        
    }
    // stage 3: the third stage of the unbuffered operation where the operation is waiting for existing operation to complete
    else if(channel->unbuffered_stage == 2){

        if(operation == UNBUFFERED_SEND)
        {
            channel->send_waiting++;
        }
        else if(operation == UNBUFFERED_RECEIVE)
        {
            channel->recv_waiting++;
        }

        // this condition is just used to block the thread until the compatible first stage operation is completed
        pthread_cond_wait(&channel->cond_waiting_stage, &channel->mutex); 
        
        if(operation == UNBUFFERED_SEND)
        {
            channel->send_waiting--;
        }
        else if(operation == UNBUFFERED_RECEIVE)
        {
            channel->recv_waiting--;
        }

        // go to stage 0 again
        goto stage0;
}

    return GENERIC_ERROR;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full or no opposite unbuffered operation is waiting, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel or successfully completing the unbuffered operation,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    if(pthread_mutex_lock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    if(channel->is_closed)
    {
        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        return CLOSED_ERROR;
    }
    
    // if the channel is unbuffered
    if(channel->unbuffered)
    {
        enum channel_status status = unbuffered_sync(channel, UNBUFFERED_SEND, &data);

        return status;
    }

    // if the channel is buffered
    else
    {
        /* IMPLEMENT THIS */

        while(buffer_add(channel->buffer, data) == BUFFER_ERROR)
        {
            if (channel->is_closed)
            {
                if(pthread_mutex_unlock(&channel->mutex) != 0)
                {
                    return GENERIC_ERROR;
                }
                return CLOSED_ERROR;
            }
            pthread_cond_wait(&channel->cond_full, &channel->mutex);
        }

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }

        signal_semaphore_select_recv(channel);
        pthread_cond_signal(&channel->cond_empty);
        
        return SUCCESS;
    }
    return GENERIC_ERROR;

}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty or no opposite unbuffered operation is waiting, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data or successfully completing the unbuffered operation,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */

    if(pthread_mutex_lock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    if(channel->is_closed)
    {
        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        return CLOSED_ERROR;
    }

    // if the channel is unbuffered
    if(channel->unbuffered)
    {
        enum channel_status status = unbuffered_sync(channel, UNBUFFERED_RECEIVE, data);

        return status;

    }

    // if the channel is buffered
    else
    {
        /* IMPLEMENT THIS */
        while(buffer_remove(channel->buffer, data) == BUFFER_ERROR)
        {

            if (channel->is_closed)
            {
                if(pthread_mutex_unlock(&channel->mutex) != 0)
                {
                    return GENERIC_ERROR;
                }
                return CLOSED_ERROR;
            }

            pthread_cond_wait(&channel->cond_empty, &channel->mutex);
        }

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }

        signal_semaphore_select_send(channel);
        pthread_cond_signal(&channel->cond_full);

        return SUCCESS;
    }
    return GENERIC_ERROR;

}

// Checks if there is a send operation waiting in the select list
bool send_waiting_in_select(channel_t* channel)
{
    pthread_mutex_lock(&channel->select_mutex);

    if(list_count(channel->semaphore_select_list_send) > 0)
    {
        pthread_mutex_unlock(&channel->select_mutex);
        return true;
    }

    pthread_mutex_unlock(&channel->select_mutex);

    return false;
}

// Checks if there is a send operation waiting in the select list
bool recv_waiting_in_select(channel_t* channel)
{
    pthread_mutex_lock(&channel->select_mutex);

    if(list_count(channel->semaphore_select_list_recv) > 0)
    {
        pthread_mutex_unlock(&channel->select_mutex);
        return true;
    }

    pthread_mutex_unlock(&channel->select_mutex);

    return false;
}


// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full or no recv operation is available to complete the unbuffered operation
// Returns SUCCESS for successfully writing data to the channel or successfully completing the unbuffered operation,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer or the unbuffered operation was not completed,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* IMPLEMENT THIS */

    if(pthread_mutex_lock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    if (channel->is_closed)
    {
        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        return CLOSED_ERROR;
    }

    // if the channel is unbuffered
    if (channel->unbuffered){
        // if the recv operation is waiting in stage 2 or 3, then wait for the operation to reach stage 1
        while(channel->recv_waiting > 0 && channel->unbuffered_stage == 0 && recv_waiting_in_select(channel) == false)
        {
            pthread_cond_wait(&channel->cond_full, &channel->mutex);
        }
        // if the recv operation is waiting in stage 1 or in select list, then complete the operation
        if ((channel->unbuffered_stage == 1 && channel->unbuffered_operation == UNBUFFERED_RECEIVE) || recv_waiting_in_select(channel) == true)
        {
            enum channel_status status = unbuffered_sync(channel, UNBUFFERED_SEND, &data);
            return status;
        }
        // if the recv operation is not waiting, then return channel full
        else
        {
            if(pthread_mutex_unlock(&channel->mutex) != 0)
            {
                return GENERIC_ERROR;
            }
            return CHANNEL_FULL;
        }

    }

    // if the channel is buffered
    else{

        if(buffer_add(channel->buffer, data) == BUFFER_ERROR)
        {
            if(pthread_mutex_unlock(&channel->mutex) != 0)
            {
                return GENERIC_ERROR;
            }
            return CHANNEL_FULL;
        }

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }

        signal_semaphore_select_recv(channel);
        pthread_cond_signal(&channel->cond_empty);
        

        return SUCCESS;

    }
    return GENERIC_ERROR;
    
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty or no send operation is available to complete the unbuffered operation
// Returns SUCCESS for successful retrieval of data or successfully completing the unbuffered operation,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data or the unbuffered operation was not completed,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* IMPLEMENT THIS */

    if(pthread_mutex_lock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    if (channel->is_closed)
    {
        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        return CLOSED_ERROR;
    }

    // if the channel is unbuffered
    if (channel->unbuffered)
    {
        // if send operation is waiting in stage 2 or 3, then wait for the operation to reach stage 1
        while(channel->send_waiting > 0 && channel->unbuffered_stage == 0 && recv_waiting_in_select(channel) == false)
        {
            pthread_cond_wait(&channel->cond_empty, &channel->mutex);
        }
        // if the send operation is waiting in stage 1 or in select list, then complete the operation
        if((channel->unbuffered_stage == 1 && channel->unbuffered_operation == UNBUFFERED_SEND) || send_waiting_in_select(channel) == true)
        {
            enum channel_status status = unbuffered_sync(channel, UNBUFFERED_RECEIVE, data);
            return status;
        }
        // if the send operation is not waiting, then return channel empty
        else
        {
            if(pthread_mutex_unlock(&channel->mutex) != 0)
            {
                return GENERIC_ERROR;
            }
            return CHANNEL_EMPTY;
        }
        
    }

    // if the channel is buffered
    else{

        if(buffer_remove(channel->buffer, data) == BUFFER_ERROR)
        {
            if(pthread_mutex_unlock(&channel->mutex) != 0)
            {
                return GENERIC_ERROR;
            }
            return CHANNEL_EMPTY;
        }

        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }

        signal_semaphore_select_send(channel);
        pthread_cond_signal(&channel->cond_full);
        
        return SUCCESS;

    }

    return GENERIC_ERROR;
    
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GENERIC_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* IMPLEMENT THIS */

    if(pthread_mutex_lock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    if(channel->is_closed)
    {
        if(pthread_mutex_unlock(&channel->mutex) != 0)
        {
            return GENERIC_ERROR;
        }
        return CLOSED_ERROR;
    }

    channel->is_closed = true;

    if(pthread_mutex_unlock(&channel->mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    signal_semaphore_select_recv(channel);
    signal_semaphore_select_send(channel);
    pthread_cond_broadcast(&channel->cond_empty);
    pthread_cond_broadcast(&channel->cond_full);
    pthread_cond_broadcast(&channel->cond_waiting_stage);
    pthread_cond_broadcast(&channel->cond_completed_stage);

    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GENERIC_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    /* IMPLEMENT THIS */

    if (channel->is_closed == false)
    {
        return DESTROY_ERROR;
    }

    pthread_cond_destroy(&channel->cond_full);
    pthread_cond_destroy(&channel->cond_empty);
    pthread_cond_destroy(&channel->cond_waiting_stage);
    pthread_cond_destroy(&channel->cond_completed_stage);
    pthread_mutex_destroy(&channel->mutex);
    pthread_mutex_destroy(&channel->select_mutex);
    if (!channel->unbuffered)
    {
        buffer_free(channel->buffer);
    }
    list_destroy(channel->semaphore_select_list_send);
    list_destroy(channel->semaphore_select_list_recv);
    free(channel);

    return SUCCESS;
}

// Add a semaphore to the select list with send operation
void add_semaphore_select_list_send(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_insert(channel->semaphore_select_list_send, semaphore);

    pthread_mutex_unlock(&channel->select_mutex);
}

// Add a semaphore to the select list with recv operation
void add_semaphore_select_list_recv(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_insert(channel->semaphore_select_list_recv, semaphore);

    pthread_mutex_unlock(&channel->select_mutex);
}

// Remove a semaphore from the select list with send operation
void remove_semaphore_select_list_send(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_remove(channel->semaphore_select_list_send, list_find(channel->semaphore_select_list_send, semaphore));

    pthread_mutex_unlock(&channel->select_mutex);
}

// Remove a semaphore from the select list with recv operation
void remove_semaphore_select_list_recv(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_remove(channel->semaphore_select_list_recv, list_find(channel->semaphore_select_list_recv, semaphore));

    pthread_mutex_unlock(&channel->select_mutex);
}

// Cleanup the select list by removing the provided semaphore
void cleanup_semaphore_select(select_t* channel_list, size_t channel_count, sem_t* semaphore)
{
    for (size_t i = 0; i < channel_count; i++)
    {
        if (channel_list[i].dir == SEND)
        {
            remove_semaphore_select_list_send(channel_list[i].channel, semaphore);
        }
        else if (channel_list[i].dir == RECV)
        {
            remove_semaphore_select_list_recv(channel_list[i].channel, semaphore);
        }
    }
}

// Initialize the select list with the provided semaphore
void init_semaphore_select(select_t* channel_list, size_t channel_count, sem_t* semaphore)
{
    for (size_t i = 0; i < channel_count; i++)
    {
        if (channel_list[i].dir == SEND)
        {
            add_semaphore_select_list_send(channel_list[i].channel, semaphore);
        }
        else if (channel_list[i].dir == RECV)
        {
            add_semaphore_select_list_recv(channel_list[i].channel, semaphore);
        }
    }
}


// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* IMPLEMENT THIS */

    // Initialize the mutex and semaphore
    pthread_mutex_t mutex;
    sem_t semaphore;
    pthread_mutex_init(&mutex, NULL);
    sem_init(&semaphore, 0, 0);

    // Check for invalid inputs
    if (channel_count == 0)
    {
        return GENERIC_ERROR;
    }

    if (channel_list == NULL)
    {
        return GENERIC_ERROR;
    }

    if(selected_index == NULL)
    {
        return GENERIC_ERROR;
    }

    if (pthread_mutex_lock(&mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    // Initialize the select list with the provided semaphore
    init_semaphore_select(channel_list, channel_count, &semaphore);
    
    while(1){

        // Iterate over the provided list and find the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
        // If multiple options are available, it selects the first option and performs its corresponding action
        for (size_t i = 0; i < channel_count; i++)
        {
            // if the operation is send
            if (channel_list[i].dir == SEND)
            {
                enum channel_status status = channel_non_blocking_send(channel_list[i].channel, channel_list[i].data);
                if (status != CHANNEL_FULL)
                {
                    *selected_index = i;

                    cleanup_semaphore_select(channel_list, channel_count, &semaphore);

                    if(pthread_mutex_unlock(&mutex) != 0)
                    {
                        return GENERIC_ERROR;
                    }

                    sem_destroy(&semaphore);
                    pthread_mutex_destroy(&mutex);

                    return status;
                }
            }
            // if the operation is receive
            else if (channel_list[i].dir == RECV)
            {
                enum channel_status status = channel_non_blocking_receive(channel_list[i].channel, &channel_list[i].data);
                if (status != CHANNEL_EMPTY)
                {
                    *selected_index = i;
                    
                    cleanup_semaphore_select(channel_list, channel_count, &semaphore);

                    if(pthread_mutex_unlock(&mutex) != 0)
                    {
                        return GENERIC_ERROR;
                    }

                    sem_destroy(&semaphore);
                    pthread_mutex_destroy(&mutex);

                    return status;
                }
            }
        }
        // If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
        sem_wait(&semaphore);
    
    }

    // if loops exits in any other way
    cleanup_semaphore_select(channel_list, channel_count, &semaphore);

    if(pthread_mutex_unlock(&mutex) != 0)
    {
        return GENERIC_ERROR;
    }

    sem_destroy(&semaphore);
    pthread_mutex_destroy(&mutex);


    return GENERIC_ERROR;
}