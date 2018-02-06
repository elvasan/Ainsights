# Created by tomlevandusky at 2/2/18
#
# A client of AIDA insights updloads a csv file consisting of the following
# fields:
#     recordid          - Customer supplied record identifier
#   * phone01,02,03     - zero or more hashed phone numbers
#   * email01,02,03,04  - zero or more hashed email addresses
#   * leadid01,02,03    - zero or more lead ids
#     asof              - the "latest" date to check
#
#   (*) At least one lead id and one of phone/email must be provided
#
Feature: Beestest
  Test the files created by AIDA Insights for a client

  Scenario: Setup
     When I set up the test for client beestest
     Then the environment is set up correctly

  Scenario: Process the client input file
     When the input file is uploaded to the S3 incoming folder
     Then the input file is processed by the system

  Scenario: Download the output zip file
     When the output zip file exists on the system
     Then the zip file is downloaded

  Scenario: The client Input Summary file is correct
     When the zip file contains a client input summary file
     Then the extracted client input summary file matches the expected file

  Scenario: The client Output Summary file is correct
     When the zip file contains a client output summary file
     Then the extracted client output summary file matches the expected file

  Scenario: The client Output Detail file is correct
     When the extracted client output detail file matches the expected file
     Then the zip file contains a client output detail file
      And the reported statuses are EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN
