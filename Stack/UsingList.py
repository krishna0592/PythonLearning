if __name__ == '__main__':
    #considering the list as stack
    stack = []
    print("Initital stack :")
    print(stack)

    # size of stack
    print("Length of stack -", len(stack))
    print()

    #addidng elements into stack
    stack.append(5)
    stack.append(3)
    stack.append(7)
    print("After adding elements into stack : ")
    print(stack)
    print("Length of stack -", len(stack))
    print()

    #popping elements from stack
    stack.pop()
    print("After popping one element from stack : ")
    print(stack)
    print("Length of stack -", len(stack))
    print()

