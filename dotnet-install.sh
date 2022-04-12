#!/bin/bash

echo Installing .NET.....

wget https://packages.microsoft.com/config/ubuntu/18.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
rm packages-microsoft-prod.deb

sudo apt update
sudo apt install -y apt-transport-https
sudo apt-get install -y dotnet-sdk-2.1
sudo apt install -y aspnetcore-runtime-2.1

