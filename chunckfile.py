# PYTHON SRIPT TO BREAK OUR FILE IN 5 CHUNKS

path = 'avocado.csv' 
input = open(path, 'r').read().split('\n') # Open the path and read the file
splitLen = int(len(input) / 5) # Split the max of line in 5slice
outputBase = 'avocado_chunk' #The name of the file
at = 1
for lines in range(0, len(input), splitLen):
    # First, get the list slice
    outputData = input[lines:lines+splitLen] 

    # Now open the output file, join the new slice with newlines
    # and write it out. Then close the file.
    output = open(outputBase + str(at) + '.csv', 'w')
    output.write('\n'.join(outputData))
    output.close()

    # Increment the counter
    at += 1