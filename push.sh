#!/bin/bash


set -e


if [ ! -d ".git" ]; then
  git init
  git branch -M main
fi


git add .


git diff --cached --quiet || git commit -m "Update project files"

# Ensure remote is correct
git remote remove origin 2>/dev/null || true
git remote add origin https://github.com/E-creator513/Wallet-Trust-Score.git


git push -u origin main