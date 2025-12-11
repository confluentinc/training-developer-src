#!/bin/bash

echo "============================================"
echo "  .NET 8.0 SDK & VS Code Extensions Setup"
echo "============================================"
echo ""

# Check if .NET is already installed
if command -v dotnet &> /dev/null; then
    INSTALLED_VERSION=$(dotnet --version 2>/dev/null)
    echo "✓ .NET SDK already installed: $INSTALLED_VERSION"
else
    echo "Installing .NET 8.0 SDK..."
    
    # Add Microsoft package repository for Ubuntu 22.04
    wget -q https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb -O packages-microsoft-prod.deb
    sudo dpkg -i packages-microsoft-prod.deb
    rm packages-microsoft-prod.deb
    
    # Install .NET 8.0 SDK
    sudo apt update
    sudo apt install -y dotnet-sdk-8.0
    
    echo "✓ .NET SDK installed: $(dotnet --version)"
fi

echo ""
echo "Installing VS Code extensions for C# development..."

# Install C# Dev Kit (includes C# extension, IntelliCode, and debugging support)
code --install-extension ms-dotnettools.csdevkit --force

# The C# Dev Kit automatically installs these dependencies:
# - ms-dotnettools.csharp (C# base extension)
# - ms-dotnettools.vscode-dotnet-runtime (.NET runtime for extensions)

echo ""
echo "============================================"
echo "  Installation Complete!"
echo "============================================"
echo ""
echo "Installed components:"
echo "  • .NET SDK: $(dotnet --version)"
echo "  • VS Code C# Dev Kit extension"
echo ""
echo "You can now debug .NET applications in VS Code."
echo "Open a .NET project and press F5 to start debugging."
echo ""
