
def sort(arr):
    # args: gets the array as input which needs to be sorted
    # return: this method prints the sorted array in the console and does not return anything
    # processing:
    #     1. compare the index position 1 & 2 and put the smallest in the left side of array and largest in the ride side of array
    #     2. Repeat the same step for next index positions (for eg: now compare 2 & 3 index position and swap the values)
    #     3. once the pointer reachers the end of array restart the above process from 0th index untill all the elements in the array is sorted.
    len = arr.__len__()
    print(arr)
    for i in range(len):
        for j in range(len-i-1):
            print(str(i)+" : "+str(j))
            if arr[j] > arr[j+1]:
                arr[j],arr[j+1] = arr[j+1],arr[j]
            print(arr)


if __name__ == '__main__':
    #array = [7,5,2,4,8,9,1]
    array = [9, 6, 2, 10, 8, 3, 1]
    sort(array)