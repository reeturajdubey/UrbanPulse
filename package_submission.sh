#!/bin/bash
echo "Packaging UrbanPulse Submission..."

# Clean up any old zip files
rm -f UrbanPulse_Final_Submission.zip

# Create the final zip containing the restructured codebase
zip -r UrbanPulse_Final_Submission.zip data/ ingestion/ processing/ report/ docker-compose.yml README.md

echo "Packaging complete! File created: UrbanPulse_Final_Submission.zip"
echo "NOTE: Please ensure you include your Video Walkthrough link in the submission portal!"
