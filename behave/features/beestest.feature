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
@valid
Feature: Beestest Happy Path
  Test the files created by AIDA Insights for a client

  Scenario: Setup
     Given I use client beestest
       And input file beestest.csv
      When the test environment is set up
      Then the input file is uploaded to the S3 incoming folder
       And the input file is processed by the system

  Scenario: Download and test the output zip file
    Given the output zip file exists on the system
     When the zip file is downloaded
     Then the input error file does not exist in the zip file
      And the extracted client input summary file matches the expected file
      And the extracted client output summary file matches the expected file
      And the extracted client output detail file matches the expected file
      And the reported statuses are EARLY_JOURNEY, LATE_JOURNEY or NOT_SEEN