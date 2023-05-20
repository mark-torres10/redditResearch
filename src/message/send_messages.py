if __name__ == "__main__":
    # load classified files
    # balance messages (ratio of 1:1 for outrage/not outrage)
    # from there, assign the messages as 1 (to message) and 0 (not to message)
    # send messages
    # for-loop, send message, and if request is successful, add to list of successful DMs
        # note: what to do if it fails? Is sending DMs atomic? Maybe we do a
        # try/except block, and if there's an error, catch it and log
