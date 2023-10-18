LABEL_COL = "label"
TO_MESSAGE_COL = "to_message"
AUTHOR_DM_SUBJECT_LINE = "Yale Researchers Looking to Learn More About Your Beliefs"
AUTHOR_DM_SCRIPT = """
Hi {name},

My research group at Yale University is interested in how people express \
themselves on social media. Would you like to answer a few questions to help \
us with our research? Your response will remain anonymous. 

You posted the following message on {date} in the {subreddit} subreddit:

{post}

(link {permalink})

Please take a moment to think about what was happening at the time you posted.
Think about who you were interacting with online and what you were reading about on Reddit.
Please answer the following regarding how you felt at the moment you posted:

1. How outraged did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
2. How happy did you feel on a 1-7 scale? (1 = not at all, 4 = somewhat, 7 = very)
3. How outraged do you think your message will appear to others? (1 = not at all, 4 = somewhat, 7 = very)
4. RIGHT NOW how outraged are you about the topic you posted about? (1 = not at all, 4 = somewhat, 7 = very)

You can simply respond with one answer per line such as:
5
1
3
4

Thank you for helping us learn more about how people engage with others on social media!
Please feel free to message us with any questions or if you're interested in learning more \
about this research.
""" # noqa

AUTHOR_PHASE_MESSAGE_IDENTIFIER_STRING = (
    "Take a moment to think about what was happening at the time you posted."
)