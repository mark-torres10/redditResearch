AUTHOR_DM_SCRIPT = """
    Hi {name},

    My research group is interested in how people express themselves on social
    media. Would you like to answer a few questions to help us with our
    research? Your response will remain anonymous. 

    You posted the following message on {date}:

    {post}

    Take a moment to think about what was happening at the time you posted.
    Think about who you were interacting with online, and what you were reading
    about on Reddit. Please answer the following regarding how you felt at the
    moment you posted:

    1. How outraged did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    2. How happy did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
    3. How outraged do you think your message will appear to others (1 = not at all, 4 = somewhat, 7 = very)
    4. RIGHT NOW how outraged are you about the topic you posted about (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
    3
    4
""" # noqa

OBSERVER_DM_SCRIPT = """
    Hi {name},

    I'm a researcher at Yale University, and my research group is interested in
    how people express themselves on social media. Would you like to answer a
    few questions to help us with our research? Your response will remain
    anonymous. 

    The following message was posted in the {subreddit_name} subreddit on {date}:
    
    {post}

    Please answer the following:

    1. How outraged did you think the message author was on a 1-7 scale?
    (1 = not at all, 4 = somewhat, 7 = very)

    2. How happy did you think the message author was on a 1-7?
    (1 = not at all, 4 = somewhat, 7 = very)

    You can simply respond with one answer per line such as:
    5
    1
"""