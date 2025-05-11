# Project State Tracker

## Project Requirements
- ONLY use containers "shahulin" (source) and "shahulout" (destination) - changing containers has legal consequences
- Database is Azure SQL, server: callcenter1.database.windows.net, database: call
- All tables must have prefix "rdt_", all stored procedures must have prefix "rds_"
- All Deepgram, Azure Storage, Azure SQL code must be in Python - asynchronous for performance
- Transcription must use direct Deepgram API calls with SAS URLs - minimal logic
- No mock solutions, no fallbacks - must fix core functionality for production
- Must check logs and tables for nulls/missing values after operations

## Current Status 
- Successfully verified shahulin container now contains 17 audio files (agricultural finance/leasing recordings)
- Fixed API key inconsistencies and standardized to use the working key
- Successfully generated SAS URL for audio file with 240-hour expiry as required
- Successfully performed direct transcription with Deepgram using SAS URL
- Received proper transcription results with all requested features
- Language detection working correctly (English detected)

## API Key Resolution
- Working Deepgram API key: ba94baf7840441c378c58ccd1d5202c38ddc42d8
- Standardized this key across all files
- Previously inconsistent keys:
  - Environment variable: 1f6c8f9cc2378ba0c6c5dd0d60d3d8713f89bfff
  - test_direct_transcription.py: d6290865c35bddd50928c5d26983769682fca987

## Available Audio Files in shahulin
- agricultural_finance_(murabaha)_understanding.mp3
- agricultural_leasing_(ijarah)_angry.mp3
- agricultural_leasing_(ijarah)_frustrated.mp3
- agricultural_leasing_(ijarah)_impatient.mp3
- agricultural_leasing_(ijarah)_kind.mp3
- agricultural_leasing_(ijarah)_neutral.mp3
- agricultural_leasing_(ijarah)_normal.mp3
- agricultural_leasing_(ijarah)_patient.mp3
- agricultural_leasing_(ijarah)_polite.mp3
- agricultural_leasing_(ijarah)_rude.mp3
- agricultural_leasing_(ijarah)_tensedup.mp3
- agricultural_leasing_(ijarah)_understanding.mp3
- business_investment_account_(mudarabah)_angry.mp3
- business_investment_account_(mudarabah)_frustrated.mp3
- business_investment_account_(mudarabah)_impatient.mp3
- business_investment_account_(mudarabah)_kind.mp3
- business_investment_account_(mudarabah)_neutral.mp3

## Database Implementation
- Azure SQL Server connected successfully
- Use two-phase database approach: first insert with NULL transcription, then update with actual data

## Technical Implementation Details
- Direct Transcription with Deepgram requires:
  1. Generate SAS URL with 240-hour expiry for blob in shahulin container
  2. Call Deepgram API with this SAS URL (no download required)
  3. Process and store results in Azure SQL database
  4. Move processed file from shahulin to shahulout

## Issues Fixed
1. Inconsistent API key in test_direct_transcription.py - FIXED
2. Empty "shahulin" container issue - RESOLVED (now contains 17 files)
3. SAS URL generation and verification - FIXED (working with 200 status)
4. Direct transcription with Deepgram - VALIDATED (successful transcript received)

## Remaining Tasks
1. Implement proper error handling in direct transcription process
2. Ensure database operations follow two-phase approach
3. Verify all tables and stored procedures follow required naming conventions
4. Implement file movement between containers after processing
5. Add comprehensive logging throughout the pipeline
6. Implement end-to-end testing of the complete workflow