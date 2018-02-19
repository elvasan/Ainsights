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
@invalid
Feature: Beestest Invalid input file
  Test the files created by AIDA Insights for a client

  Scenario: Setup
    Given I use client beestest
      And input file beestest-invalid.csv
     When the test environment is set up
     Then the input file is uploaded to the S3 incoming folder
      And the input file is processed by the system

  Scenario: Download and test the output zip file
    Given the output zip file exists on the system
     When the zip file is downloaded
     Then the extracted client input error file matches the expected file
      And the input summary file does not exist in the zip file
      And the output summary file does not exist in the zip file
      And the output detail file does not exist in the zip file
