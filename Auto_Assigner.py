import pandas as pd
import gspread as gs
import time
from datetime import datetime
from jira import JIRA


class Worker():
    def __init__(self, name, team, email, tickets_amount):
        self.name = name
        self.team = team
        self.email = email
        self.tickets_amount = tickets_amount


def login():
    """ Returning jira object for using the Jira api.
    Asking for email adress and and Jira token."""
    email = str(input("Please enter your email:\n"))
    jira_token = str(input("\nPlease enter you jira api token:\n"))
    jira = JIRA(basic_auth=(email, jira_token), options={'server': "https://seetree.atlassian.net/"})
    return jira


def get_general_manpower():
    """ Returning a dataframe of workers based on google sheets file with workers names and their emails."""
    gc = gs.service_account(filename='service_account_seetree.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1N7FCFnOKoQvKTqPgI1RCEOW5j8pVpZI9LDWiuoxlnkc/edit#gid=2118451581')
    ws = sh.worksheet('Team info - Ops Team')
    workers = pd.DataFrame(ws.get_all_records())
    return workers


def num_validation(num_as_string, values_range):
    """ Returning an int variable after checking that it's in the wanted range.
        Asking for a string and a values range"""
    while True:
        try:
            num = int(num_as_string)
            if num not in range(values_range):
                print(f"\nPlease choose a number between 0 to {values_range-1}")
                continue
        except ValueError:
            print("\nYou did not enter a number\n")
            continue
        except:
            print("\nSomething went wrong. Please try again\n")
            continue
        else:
            print("Ok! Got it")
            break
    return num

def team_selector(workers):
    """ Returning a string with the relevant team between the teams in the workers dataframe.
   Asking for workers data frame."""
    teams = list(workers["Team"].unique())
    teams.sort()
    enumerate_teams = list(enumerate(teams))
  
    print("\nPlease choose the relevant team:")
    for team in enumerate_teams:
        print(f"{team[0]} - for the {team[1]} team")
    user_choose = input()
    user_choose = num_validation(user_choose, len(teams))
    return teams[user_choose]


def absent_people(workers, team):
    """ Returning a list with the available workers in the specific working day.
   Asking workers dataframe and the chosen team."""
    workers_by_teams = workers.set_index("Team")
    workers_from_relevant_team = workers_by_teams.loc[[team], ["Name"]]
    available_workers = []
    zero_or_one = 2

    # Checking if someone is missing
    absence = input("\nDoes someone is missing today from the {} team?\n0 for No\n1 for Yes\n".format(team))
    absence = num_validation(absence, zero_or_one)
            
    # If someone is missing        
    if absence == 1:  
        for team, name in workers_from_relevant_team.itertuples():
            is_working = input(f"Does {name} is working today?\n0 for No\n1 for Yes\n")
            is_working = num_validation(is_working, zero_or_one)
            if is_working == 1: 
                available_workers.append(name)
            else:
                continue
                    
    # If no-one missing
    else: 
        for team, name in workers_from_relevant_team.itertuples():
            available_workers.append(name)
        print("\nThe team today:")
        for worker in available_workers:
            print(worker)
            
    return available_workers


def get_new_guys(workers, available_workers, team):
    """ Return a list of workers from the original chosen team + guys that helping to the original team.
    Asking for workers dataframe, list of the available workers from the original team and the origianl team's name."""

    zero_or_one = 2
    new_guys = available_workers.copy()

    # Checks if workers from others teams are joining.
    new_people = input("\nWill you have workers from other teams?\n0 for No\n1 for Yes\n")
    new_people = num_validation(new_people, zero_or_one)

    # If someone from other team is joining
    if new_people == 1:  
        print("\nPlease choose the new guy's team:")
        while True:
            new_guy_team = team_selector(workers)

            # Checking ff the user chose by accident the original team as the team where new guys will join.
            if new_guy_team == team: 
                print("\nPlease choose a different team from tha main team that you chose!\n")
                continue
            else:
                break
        
        workers_by_teams = workers.set_index("Team")
        workers_from_relevant_team = workers_by_teams.loc[[new_guy_team], ["Name"]]

        # Iterate through the new guy's team
        for team, name in workers_from_relevant_team.itertuples():  
            new_guy = input(f"\nWill {name} join to your team?\n0 for No\n1 for Yes\n")
            new_guy = num_validation(new_guy, zero_or_one)
            if new_guy == 1:  # Adding the new guy to the new_guys lists.
                new_guys.append(name)
            elif new_guy == 0:  # Moving to the next guy from the chosen team lists.
                continue

        # If guys from different teams will join either
        other_team = input("\nWill someone from another team will join to your team?\n0 for No\n1 for Yes\n")
        other_team = num_validation(other_team, zero_or_one)

        # If no one else will join.
        if other_team == 0: 
            return new_guys
        # If guys from different teams will join.
        else: 
            new_guys = get_new_guys(workers, new_guys, team)
            return new_guys

    # If no one new is joining
    else:
        return new_guys


def init_full_available_team(full_available_team_names, team, team_filter_jql, jira_user, workers):
    """ Returning a list off all the relevant daily workers as objects.
   Asking for:
   full available team names as a list, team as a list, team filter jql as a list, jira user object and workers dataframe."""

    available_team = []

    for worker in full_available_team_names:
        get_email = workers.set_index("Name").loc[[worker], ["Email"]]
        for name, email in get_email.itertuples():
            get_key = jira_user._get_user_id(email)
            amount_of_tickets = get_users_tickets_amount(worker, get_key, team_filter_jql, jira_user)
            x = Worker(worker, team, email,amount_of_tickets)
            available_team.append(x)
    return available_team
            

def get_users_tickets_amount(user_name, user_key, team_jql, jira_user):
    """ Returning the amount of tickets that each user has.
   Asking for:
   user user's name as a list, user's key as a list, team_jql as a list, and Jira user as jira object. """

    print("\n" + user_name)
    jql = team_jql.split("ORDER") # Splitting the jql between to logic side to the order side
    jql_specific_user = jql[0] + f"And assignee = {user_key} ORDER" + jql[1] # Adding to the jql a logic for find the specific user's tickets.
    print(jql[0] + f"And assignee = {user_key} ORDER" + jql[1])

    user_tickets = get_jql_tickets(jql_specific_user, jira_user)
    user_tickets_amount = len(user_tickets)

    if user_tickets_amount == 0:  # While a worker has no tickets
        print(f"{user_name} has no tickets!")
        time.sleep(2)
    else: # While a worker has tickets
        print(str(user_name) + f"'s amount of tickets: {user_tickets_amount}")
        time.sleep(2)
    return user_tickets_amount


def get_jql_tickets(jql, jira_user):
    """ Returning list of the tickets in the jql.
        Asking for a jql string and jira object"""

    num_of_tickets = 0
    ticket_list = []
    res = jira_user.search_issues(jql)

    if not res:
        print("There are no tickets here!")
    else:
        print("These are the tickets the jql:")
        for issue in res:  # Creates list of the tickets in the input jql
            ticket_list.append(issue)
            print(ticket_list[num_of_tickets])
            num_of_tickets += 1
    return ticket_list


def get_general_filters():
    """ Returning a dataframe of filters based on google sheets file with teams and their Jira filter number. """

    gc = gs.service_account(filename='service_account_seetree.json')
    sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1N7FCFnOKoQvKTqPgI1RCEOW5j8pVpZI9LDWiuoxlnkc/edit#gid=2118451581')
    ws = sh.worksheet('Teams Filters')
    filters = pd.DataFrame(ws.get_all_records())
    return filters


def get_jql(filter_id, jira):
    """ Returning filer jql as a string. 
    Asking filterID numnber as a string and jira object."""

    jql = jira.filter(filter_id).raw["jql"]
    return jql


def get_current_time():
    """Returning the current hour as int, in 24 format time"""

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    current_hour = int(current_time[0:2])
    return current_hour


def find_least_assignments_man(metadata_full_available_team):
    """Returning the worker with the lowset amount of tickets in the relevant team.
    Asking for the workers metadata list."""

    min_worker = metadata_full_available_team[0]
    for worker in metadata_full_available_team:
        if worker.tickets_amount < min_worker.tickets_amount:
            min_worker = worker
    print(
        f"\n{min_worker.name} has the lowest amount of tickets! He has {min_worker.tickets_amount} tickets.")
    return min_worker.email


def assignee_oldest_ticket(min_worker, jira_user, team_filter_jql):
    """Assigning the ticket with the highest latency to the person with the least amount of tickets"""

    splited_team_filter = team_filter_jql.split("ORDER")
    jql_for_unassigned_tickets = splited_team_filter[0] + "AND assignee = EMPTY ORDER" + splited_team_filter[1]
    unassigneed_issues_list = get_jql_tickets(jql_for_unassigned_tickets, jira_user)

    if not unassigneed_issues_list: # If there are no unassigned issues
        print("\nCurrently, there are no unassigned tickets.")
        return
    highest_days_since_shooting_ticket = str(unassigneed_issues_list[0])
    jira_user.assign_issue(highest_days_since_shooting_ticket, min_worker)


def main():
    jira_user = login()
    workers = get_general_manpower()
    team = team_selector(workers)
    available_workers = absent_people(workers, team)
    full_available_team_names = get_new_guys(workers, available_workers,team)
    filters = get_general_filters().set_index("Team")
    team_filter_id = str(filters.loc[[team]].values[0][0])
    team_filter_jql = get_jql(team_filter_id, jira_user)
    metadata_full_available_team = init_full_available_team(full_available_team_names,team, team_filter_jql, jira_user, workers)
    min_worker = find_least_assignments_man(metadata_full_available_team)
    assignee_oldest_ticket(min_worker, jira_user, team_filter_jql)
    current_time = get_current_time()
    while (current_time >= 9) and (current_time <= 18):
        min_worker = find_least_assignments_man(metadata_full_available_team)
        assignee_oldest_ticket(min_worker, jira_user, team_filter_jql)
        for sec in range(300):
            sec_left = 300 - sec
            time.sleep(1)
            print(f"\nChecking again in {sec_left} seconds\n")
        current_time = get_current_time()


if __name__ == "__main__":
    main()
