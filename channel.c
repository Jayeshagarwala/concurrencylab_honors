#include "channel.h"

// Creates a new channel with the provided size and returns it to the caller
channel_t* channel_create(size_t size)
{
    /* IMPLEMENT THIS */

    channel_t* channel = (channel_t*) malloc(sizeof(channel_t));

    channel->buffer = buffer_create(size);
    pthread_mutex_init(&channel->mutex, NULL);
    pthread_mutex_init(&channel->select_mutex, NULL);
    pthread_cond_init(&channel->cond_full, NULL);
    pthread_cond_init(&channel->cond_empty, NULL);
    channel->is_closed = false;
    channel->semaphore_select_list = list_create();

    return channel;
}

// Signal all the semaphores in the select list
// This function is called whenever a send or receive operation is successful
// This function is also called when the channel is closed
void signal_semaphore_select(channel_t* channel)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_node_t* node = list_head(channel->semaphore_select_list);

    while (node != NULL)
    {
        sem_post((sem_t*)node->data);
        node = node->next;
    }
        
    pthread_mutex_unlock(&channel->select_mutex);
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
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

    signal_semaphore_select(channel);
    pthread_cond_signal(&channel->cond_empty);
    

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
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

    signal_semaphore_select(channel);
    pthread_cond_signal(&channel->cond_full);

    return SUCCESS;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
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

    signal_semaphore_select(channel);
    pthread_cond_signal(&channel->cond_empty);
    

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
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

    signal_semaphore_select(channel);
    pthread_cond_signal(&channel->cond_full);
    
    return SUCCESS;
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

    signal_semaphore_select(channel);
    pthread_cond_broadcast(&channel->cond_empty);
    pthread_cond_broadcast(&channel->cond_full);

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
    pthread_mutex_destroy(&channel->mutex);
    pthread_mutex_destroy(&channel->select_mutex);
    buffer_free(channel->buffer);
    list_destroy(channel->semaphore_select_list);
    free(channel);

    return SUCCESS;
}

// Add a semaphore to the select list
void add_semaphore_select_list(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_insert(channel->semaphore_select_list, semaphore);

    pthread_mutex_unlock(&channel->select_mutex);
}

// Remove a semaphore from the select list
void remove_semaphore_select_list(channel_t* channel, sem_t* semaphore)
{
    pthread_mutex_lock(&channel->select_mutex);

    list_remove(channel->semaphore_select_list, list_find(channel->semaphore_select_list, semaphore));

    pthread_mutex_unlock(&channel->select_mutex);
}

// Cleanup the select list by removing the provided semaphore
void cleanup_semaphore_select(select_t* channel_list, size_t channel_count, sem_t* semaphore)
{
    for (size_t i = 0; i < channel_count; i++)
    {
        remove_semaphore_select_list(channel_list[i].channel, semaphore);
    }
}

// Initialize the select list with the provided semaphore
void init_semaphore_select(select_t* channel_list, size_t channel_count, sem_t* semaphore)
{
    for (size_t i = 0; i < channel_count; i++)
    {
        add_semaphore_select_list(channel_list[i].channel, semaphore);
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