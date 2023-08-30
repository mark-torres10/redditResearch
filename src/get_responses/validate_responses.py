"""Validate the responses that were provided by users.

Performs validation for one of the phases (author/observer). Does validation
only for the messages that weren't previously validated.
"""
import argparse
import os
from typing import Dict, List

import pandas as pd

from get_responses import constants



def get_previous_labeling_session_data() -> List[Dict]:
    """Load in previous labeling session data. For each session, load
    as a pandas df and then convert to a list of dicts."""
    list_previously_labeled_data = []
    for filename in os.listdir(constants.VALIDATED_RESPONSES_ROOT_PATH):
        if "VALIDATED_RESPONSES_ROOT_FILENAME" in filename:
            df = pd.DataFrame(
                os.path.join(constants.VALIDATED_RESPONSES_ROOT_PATH, filename)
            )
            list_dict = df.to_dict()
            list_previously_labeled_data.extend(list_dict)
    return list_previously_labeled_data


def load_previously_labeled_ids() -> List[str]:
    previously_labeled_data = []
    load_dir = constants.VALIDATED_RESPONSES_ROOT_PATH
    for filepath in os.listdir(load_dir):
        df = pd.read_csv(os.path.join(load_dir, filepath))
        ids = df["message_id"].tolist()
        previously_labeled_data.extend(ids)

    return previously_labeled_data


def load_messages(phase: str) -> List[Dict]:
    """Loads previously dumped messages for a given phase.
    
    Returns a list of dictionaries where each dictionary has as its keys
    the `id` and `body`, corresponding to the message ID and body respectively.
    """
    # get the max timestamp out of the directories available in the root phase
    # directory. This will have the most up-to-date load
    root_dir = os.path.join(constants.RESPONSES_ROOT_PATH, phase)
    max_timestamp_dir = max(os.listdir(root_dir))

    messages_df = pd.read_csv(
        os.path.join(
            root_dir,
            max_timestamp_dir,
            constants.ALL_RESPONSES_FILENAME.format(phase=phase)
        )
    )

    # transform df to a list of dicts
    messages = messages_df.to_dict("records")

    return messages


def write_labels_to_csv(messages_with_validation_status: List[Dict]) -> None:
    """Writes the validated messages as a .csv file."""
    colnames = ["id", "phase", "is_valid_response", "scores"]
    df = pd.DataFrame(messages_with_validation_status, columns=colnames)
    df.to_csv(constants.SESSION_VALIDATED_RESPONSES_FILEPATH)


def manually_validate_messages(phase: str, messages: List[Dict]) -> List[Dict]:
    """Validates the messages that we have received.
    
    Manually QAs the messages and records the ones that have valid responses.
    """
    previously_labeled_data_ids = load_previously_labeled_ids()

    num_messages = len(messages)
    responses_list: List[Dict] = []

    for idx, msg in enumerate(messages):
        print('-' * 10)
        print(f"Labeling message {idx} out of {num_messages}")
        # print the body
        print(f"Body: {msg.body}")
        # ask if valid (y/n) or if to exit session
        user_input = ''
        valid_inputs = ['y', 'n', 'e']
        break_session = False
        if msg.id in previously_labeled_data_ids:
            print(f"Previously labeled response, with id {msg.id}, skipping")
            continue
        while user_input not in valid_inputs:
            user_input = input("Is this a valid response? ('y', 'n', or 'e' to exit)\t") # noqa
            if user_input == 'y':
                print("Valid response.")
                scores = input("Please enter their scores (e.g., 1123):\t")
                responses_list.append({
                    "id": msg.id,
                    "phase": phase,
                    "is_valid_response": 1,
                    "scores": scores
                })

            elif user_input == 'n':
                print("Invalid response")
                responses_list.append({
                    "id": msg.id,
                    "phase": phase,
                    "is_valid_response": 0,
                    "scores": ''
                })
            elif user_input == 'e':
                print("Exiting labeling session...")
                break_session = True
            else:
                print(f"Invalid input: {user_input}")
        user_input = ''
        if break_session:
            break

    return responses_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for validating DMs received on Reddit."
    )
    parser.add_argument(
        "--phase", type=str, required=True, help="Phase (author/observer)"
    )
    args = parser.parse_args()
    messages = load_messages(phase=args.phase)
    messages_with_validation_status = manually_validate_messages(
        messages=messages
    )
    write_labels_to_csv(messages_with_validation_status)
