#include <stdlib.h>
#include "linked_list.h"

// Creates and returns a new list
list_t* list_create()
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    
    list_t* new_list = (list_t*)malloc(sizeof(list_t));

    // Error handling for failed memory allocation
    if (new_list == NULL) {
        return NULL;
    }

    new_list->head = NULL;
    new_list->tail = NULL;
    new_list->count = 0;
    return new_list;
}

// Destroys a list
void list_destroy(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    list_node_t* current = list->head;
    list_node_t* next;
    while (current) {
        next = current->next;
        free(current);
        current = next;
    }
    free(list);
}

// Returns head of the list
list_node_t* list_head(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return list->head;
}

// Returns tail of the list
list_node_t* list_tail(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return list->tail;
}

// Returns next element in the list
list_node_t* list_next(list_node_t* node)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return node->next;
}

// Returns prev element in the list
list_node_t* list_prev(list_node_t* node)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return node->prev;
}

// Returns end of the list marker
list_node_t* list_end(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return list->tail;
}

// Returns data in the given list node
void* list_data(list_node_t* node)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return node->data;
}

// Returns the number of elements in the list
size_t list_count(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return list->count;
}

// Finds the first node in the list with the given data
// Returns NULL if data could not be found
list_node_t* list_find(list_t* list, void* data)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    
    list_node_t* current = list->head;
    while (current != NULL) {
        if (current->data == data) {
            return current;
        }
        current = current->next;
    }
    return NULL;
}

// Inserts a new node in the list with the given data
// Returns new node inserted
list_node_t* list_insert(list_t* list, void* data)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    
    list_node_t* new_node = (list_node_t*)malloc(sizeof(list_node_t));
    if (new_node == NULL) {
        return NULL;
    }
    new_node->data = data;
    new_node->next = NULL;
    new_node->prev = NULL;
    if (list->head == NULL) {
        list->head = new_node;
        list->tail = new_node;
        list->count++;
        return new_node;
    }
    list->tail->next = new_node;
    new_node->prev = list->tail;
    list->tail = new_node;
    list->count++;
    return new_node;
}

// Removes a node from the list and frees the node resources
void list_remove(list_t* list, list_node_t* node)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */

    if (node == list->head) {
        list->head = node->next;
    }
    if (node == list->tail) {
        list->tail = node->prev;
    }
    if (node->prev != NULL) {
        node->prev->next = node->next;
    }
    if (node->next != NULL) {
        node->next->prev = node->prev;
    }

    list->count--;
    node->next = NULL;
    node->prev = NULL;
    free(node);
}
