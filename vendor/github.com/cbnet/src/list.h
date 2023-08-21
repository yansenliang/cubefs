/*
* Copyright (c) NVIDIA CORPORATION & AFFILIATES, 2001-2014. ALL RIGHTS RESERVED.
*
* See file LICENSE for terms.
*/

#ifndef LIST_H_
#define LIST_H_

/**
 * @return Offset of _member in _type. _type is a structure type.
 */
#ifndef offsetof
#define offsetof(_type, _member) \
    ((unsigned long)&( ((_type*)0)->_member ))
#endif

/**
 * Get a pointer to a struct containing a member.
 *
 * @param _ptr     Pointer to the member.
 * @param _type    Container type.
 * @param _member  Element member inside the container.

 * @return Address of the container structure.
 */
#define container_of(_ptr, _type, _member) \
    ( (_type*)( (char*)(void*)(_ptr) - offsetof(_type, _member) )  )


/**
 * Get the type of a structure or variable.
 * 
 * @param _type  Return the type of this argument.
 * 
 * @return The type of the given argument.
 */
#define typeof(_type) \
    __typeof__(_type)


/**
 * @return Address of a derived structure. It must have a "super" member at offset 0.
 * NOTE: we use the built-in offsetof here because we can't use offsetof() in
 *       a constant expression.
 */
#define derived_of(_ptr, _type) \
    ({\
        STATIC_ASSERT(offsetof(_type, super) == 0) \
        container_of(_ptr, _type, super); \
    })


#define LIST_INITIALIZER(_prev, _next) \
    { (_prev), (_next) }


/**
 * Declare an empty list
 */
#define LIST_HEAD(name) \
    list_link_t name = LIST_INITIALIZER(&(name), &(name))


/**
 * A link in a circular list.
 */
typedef struct list_link {
    struct list_link  *prev;
    struct list_link  *next;
} list_link_t;


/**
 * Initialize list head.
 *
 * @param head  List head struct to initialize.
 */
static inline void list_head_init(list_link_t *head)
{
    head->prev = head->next = head;
}

/**
 * Insert an element in-between to list elements. Any elements which were in this
 * section will be discarded.
 *
 * @param prev Element to insert after
 * @param next Element to insert before.
 */
static inline void list_insert_replace(list_link_t *prev,
                                           list_link_t *next,
                                           list_link_t *elem)
{
    elem->prev = prev;
    elem->next = next;
    prev->next = elem;
    next->prev = elem;
}

/**
 * Replace an element in a list with another element.
 *
 * @param elem         Element in the list to replace.
 * @param replacement  New element to insert in place of 'elem'.
 */
static inline void list_replace(list_link_t *elem,
                                    list_link_t *replacement)
{
    list_insert_replace(elem->prev, elem->next, replacement);
}

/**
 * Insert an item to a list after another item.
 *
 * @param pos         Item after which to insert.
 * @param new_link    Item to insert.
 */
static inline void list_insert_after(list_link_t *pos,
                                         list_link_t *new_link)
{
    list_insert_replace(pos, pos->next, new_link);
}

/**
 * Insert an item to a list before another item.
 *
 * @param pos         Item before which to insert.
 * @param new_link    Item to insert.
 */
static inline void list_insert_before(list_link_t *pos,
                                          list_link_t *new_link)
{
    list_insert_replace(pos->prev, pos, new_link);
}

/**
 * Remove an item from its list.
 *
 * @param link  Item to remove.
 */
static inline void list_del(list_link_t *elem)
{
    elem->prev->next = elem->next;
    elem->next->prev = elem->prev;
    list_head_init(elem);
}

/**
 * @return Whether the list is empty.
 */
static inline int list_is_empty(list_link_t *head)
{
    return head->next == head;
}

/**
 * @return Whether the list contains only elem.
 */
static inline int list_is_only(list_link_t *head,
                                   list_link_t *elem)
{
    return (head->next == elem) && (elem->next == head);
}

/**
 * Move the items from 'newlist' to the tail of the list pointed by 'head'
 *
 * @param head       List to whose tail to add the items.
 * @param newlist    List of items to add.
 *
 * @note The contents of 'newlist' is left unmodified.
 */
static inline void list_splice_tail(list_link_t *head,
                                        list_link_t *newlist)
{
    list_link_t *first, *last, *tail;

    if (list_is_empty(newlist)) {
        return;
    }

    first = newlist->next; /* First element in the new list */
    last  = newlist->prev; /* Last element in the new list */
    tail  = head->prev;    /* Last element in the original list */

    first->prev = tail;
    tail->next = first;

    last->next = head;
    head->prev = last;
}

/**
 * Count the members of the list
 */
static inline unsigned long list_length(list_link_t *head)
{
    unsigned long length;
    list_link_t *ptr;

    for (ptr = head->next, length = 0; ptr != head; ptr = ptr->next, ++length);
    return length;
}

/*
 * Convenience macros
 */
#define list_add_head(_head, _item) \
    list_insert_after(_head, _item)
#define list_add_tail(_head, _item) \
    list_insert_before(_head, _item)

/**
 * Get the previous element in the list
 */
#define list_prev(_elem, _type, _member) \
    (container_of((_elem)->prev, _type, _member))

/**
 * Get the next element in the list
 */
#define list_next(_elem, _type, _member) \
    (container_of((_elem)->next, _type, _member))

/**
 * Get the first element in the list
 */
#define list_head   list_next

#define list_head_with_null(_elem, _type, _member)  list_is_empty(_elem) ? null :  list_next(_elem, _type, _member)

/**
 * Get the last element in the list
 */
#define list_tail   list_prev

/**
 * Iterate over members of the list.
 */
#define list_for_each(_elem, _head, _member) \
    for (_elem = container_of((_head)->next, typeof(*_elem), _member); \
        &(_elem)->_member != (_head); \
        _elem = container_of((_elem)->_member.next, typeof(*_elem), _member))

/**
 * Iterate over members of the list, the user may invalidate the current entry.
 */
#define list_for_each_safe(_elem, _telem, _head, _member) \
    for (_elem = container_of((_head)->next, typeof(*_elem), _member), \
        _telem = container_of(_elem->_member.next, typeof(*_elem), _member); \
        &_elem->_member != (_head); \
        _elem = _telem, \
        _telem = container_of(_telem->_member.next, typeof(*_telem), _member))

/**
 * Extract list head
 */
#define list_extract_head(_head, _type, _member) \
    ({ \
        list_link_t *tmp = (_head)->next; \
        list_del(tmp); \
        container_of(tmp, _type, _member); \
    })


#endif
