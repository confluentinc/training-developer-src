# Complete Software Version Manifest

This document lists ALL software versions installed across the Ubuntu, CP, and CP-Docker AMIs.

**Last Updated:** December 10, 2025  
**Base OS:** Ubuntu 22.04 LTS (Jammy Jellyfish)  
**Confluent Platform:** 8.1.1

---

## Table of Contents

- [Ubuntu Base AMI](#ubuntu-base-ami)
- [CP AMI](#cp-ami)
- [CP-Docker AMI](#cp-docker-ami)
- [Version Notes](#version-notes)

---

## Ubuntu Base AMI

### Operating System
| Component | Version | Notes |
|-----------|---------|-------|
| Ubuntu | 22.04 LTS | Jammy Jellyfish |
| Linux Kernel | Latest available | Auto-updated during build |

### System Packages
| Package | Version | Purpose |
|---------|---------|---------|
| `acl` | Latest | User permission management |
| `curl` | Latest | HTTP client |
| `vim` | Latest | Text editor |
| `jq` | Latest | JSON processor |
| `gnupg` | Latest | Encryption and signing |
| `software-properties-common` | Latest | Repository management |
| `expect` | Latest | Automation tool |
| `apt-transport-https` | Latest | Secure package transport |

### Desktop Environment
| Component | Version | Notes |
|-----------|---------|-------|
| `ubuntu-desktop` | Latest | Full Ubuntu Desktop |
| GNOME | 42.x | Default desktop environment |
| `xrdp` | Latest | Remote Desktop Protocol server |

### Development Tools & Languages

#### Python
| Component | Version | Installation Method |
|-----------|---------|-------------------|
| Python 3 | 3.10.x | APT (system default) |
| `pip` | Latest | APT |
| `virtualenv` | Latest | APT |
| `psutil` | Latest | pip (for dconf) |

#### Java
| Component | Version | Installation Method |
|-----------|---------|-------------------|
| OpenJDK | 21 LTS | APT |
| Maven | Latest | APT |

#### .NET
| Component | Version | Installation Method |
|-----------|---------|-------------------|
| .NET SDK | 8.0 LTS | Microsoft APT Repository |
| .NET Runtime | 8.0 | Microsoft APT Repository |

### Database Clients
| Tool | Version | Purpose |
|------|---------|---------|
| `postgresql-client` | Latest | PostgreSQL client tools |
| `sqlite3` | Latest | SQLite database engine |

### Kafka Tools
| Tool | Version | Installation Method |
|------|---------|-------------------|
| `kafkacat` (kcat) | Latest | APT |

### Web Browser
| Component | Version | Configuration |
|-----------|---------|---------------|
| Google Chrome | Stable (Latest) | Managed policies enabled |

**Chrome Managed Policies:**
- Default search engine: Google
- Auto-updates: Disabled
- Update notifications: Disabled
- Privacy Sandbox: Disabled
- Unsupported OS warnings: Suppressed

### Docker & Container Tools
| Component | Version | Installation Method |
|-----------|---------|-------------------|
| Docker CE | Latest | geerlingguy.docker role |
| Docker Compose | 2.24.0 | Plugin (supports API 1.44+) |
| Docker Compose Plugin | Enabled | Native `docker compose` command |

**Docker Configuration:**
- API Version: 1.44+
- Client timeout: 600 seconds
- Compose timeout: 600 seconds

### Code Editor
| Component | Version | Extensions |
|-----------|---------|-----------|
| Visual Studio Code | Latest | See extensions below |

**VS Code Extensions:**
- `ms-azuretools.vscode-docker` - Docker support
- `ms-python.python` - Python support
- `redhat.vscode-yaml` - YAML support

**Note:** Java and C# extensions excluded due to Node.js/V8 installation issues (can be installed manually post-build).

**VS Code Settings:**
- Minimap: Disabled
- Telemetry: Disabled
- Auto-update: Disabled
- Extension auto-update: Disabled

### HashiCorp Tools
| Tool | Version | Installation Method |
|------|---------|-------------------|
| Terraform | Latest | HashiCorp APT Repository |

### Fonts & Media
| Package | Version | Purpose |
|---------|---------|---------|
| `fonts-takao-mincho` | Latest | Japanese fonts |
| `fonts-emojione` | Latest | Emoji support |
| `imagemagick` | Latest | Image processing |

### System Libraries
| Library | Version | Purpose |
|---------|---------|---------|
| `libcanberra-gtk-module` | Latest | GTK sound theme |
| `libcanberra-gtk3-module` | Latest | GTK3 sound theme |
| `gir1.2-gtop-2.0` | Latest | System monitor library |
| `gir1.2-nm-1.0` | Latest | NetworkManager library |
| `gir1.2-clutter-1.0` | Latest | Graphics library |

---

## CP AMI

### Confluent Platform (APT Packages)

**Repository:** `https://packages.confluent.io/deb/8.1`

#### Core Platform
| Package | Version | Notes |
|---------|---------|-------|
| `confluent-platform` | 8.1.1-1 | Full Confluent Platform |
| `confluent-security` | 8.1.1-1 | Security features |
| `confluent-librdkafka-plugins` | Latest | Optional (if available) |

**Components Included:**
- Apache Kafka (KRaft mode)
- Kafka Connect
- Schema Registry
- ksqlDB
- Kafka REST Proxy
- Confluent Control Center
- Replicator
- Security features (RBAC, encryption, etc.)

#### Confluent Clients Repository

**Repository:** `https://packages.confluent.io/clients/deb`

Provides `librdkafka` client libraries (optional).

### Build Tools

| Tool | Version | Installation Method | Purpose |
|------|---------|-------------------|---------|
| Gradle | 8.12 | Direct download | Java build tool |

### Kafka Development Tools

| Tool | Version | Installation Method | Purpose |
|------|---------|-------------------|---------|
| Kafka Connect Datagen | 0.6.7 | Confluent Hub | Test data generation |

**Datagen Connectors:**
- Orders connector
- Ratings connector
- Users connector
- Clickstream connector
- Inventory connector
- And more...

### Data Directories

Pre-created directories with proper ownership:

| Directory | Owner | Purpose |
|-----------|-------|---------|
| `/var/lib/kafka/data` | cp-kafka | Kafka data logs |
| `/var/lib/control-center/data` | cp-control-center | Control Center data |
| `/home/training/docker-compose/vol/config` | training | Prometheus/Alertmanager config |

### System Users & Groups

| User/Group | Purpose |
|------------|---------|
| `confluent` | Group for CP components |
| `cp-kafka` | Kafka broker user |
| `cp-control-center` | Control Center user |
| `cp-schema-registry` | Schema Registry user |
| `cp-connect` | Kafka Connect user |
| `cp-ksqldb` | ksqlDB user |
| `cp-kafka-rest` | REST Proxy user |

### Configuration Files

| File | Purpose |
|------|---------|
| `/etc/kafka/*` | Kafka configuration |
| `/etc/schema-registry/*` | Schema Registry configuration |
| `/etc/kafka-connect/*` | Kafka Connect configuration |
| `/etc/ksqldb/*` | ksqlDB configuration |
| `/etc/confluent-control-center/*` | Control Center configuration |

---

## CP-Docker AMI

### Confluent Platform Docker Images

**Source:** Docker Hub (`hub.docker.com/u/confluentinc`)

#### Core Components
| Image | Tag | Purpose |
|-------|-----|---------|
| `confluentinc/cp-server` | 8.1.1 | Kafka broker (enterprise) |
| `confluentinc/cp-kafka` | 8.1.1 | Kafka broker (community) |
| `confluentinc/cp-schema-registry` | 8.1.1 | Schema Registry |
| `confluentinc/cp-kafka-connect` | 8.1.1 | Kafka Connect |
| `confluentinc/cp-ksqldb-server` | 8.1.1 | ksqlDB server |
| `confluentinc/cp-kafka-rest` | 8.1.1 | Kafka REST Proxy |

#### Enterprise Monitoring
| Image | Tag | Purpose |
|-------|-----|---------|
| `confluentinc/cp-enterprise-prometheus` | 2.3.0 | Prometheus with CP integration |
| `confluentinc/cp-enterprise-alertmanager` | 2.3.0 | Alertmanager with CP integration |
| `confluentinc/cp-enterprise-control-center-next-gen` | 2.3.0 | Control Center (Next-Gen UI) |

**Note:** Control Center Next-Gen uses separate versioning from Confluent Platform.

#### Utilities
| Image | Tag | Purpose |
|-------|-----|---------|
| `confluentinc/cp-kcat` | 8.1.1 | kcat (formerly kafkacat) |
| `postgres` | 16-alpine | PostgreSQL for demos |

### Removed Images (Not Available for CP 8.1)

The following images are **NOT** pulled as they are unavailable or not needed:
- `confluentinc/cp-zookeeper` - Not needed (KRaft mode)
- `confluentinc/cp-ksqldb-cli` - Not available for 8.1.1
- `confluentinc/cp-enterprise-control-center` (old name) - Replaced by Next-Gen

---

## Version Notes

### Version Format Differences

| System | Format | Example | Reason |
|--------|--------|---------|--------|
| **APT Packages** | `MAJOR.MINOR.PATCH-REVISION` | `8.1.1-1` | Debian convention |
| **Docker Images** | `MAJOR.MINOR.PATCH` | `8.1.1` | Semantic versioning |
| **Control Center** | `MAJOR.MINOR.PATCH` | `2.3.0` | Separate product line |

### Control Center Versioning

Control Center Next-Gen uses **independent versioning**:
- CP 8.1.0 → Control Center 2.3.0 (or later)
- CP 8.1.1 → Control Center 2.3.0 (or later)

The Control Center version is **not** tied to the CP version.

### Compatibility Notes

1. **Java 21**: Required for CP 8.1+ (OpenJDK 11 and 17 removed)
2. **KRaft Mode**: CP 8.0+ uses KRaft (ZooKeeper deprecated)
3. **Ubuntu 22.04**: Tested and supported LTS version
4. **Docker API 1.44+**: Required for `docker-compose` v2.24.0
5. **.NET 8.0**: Current LTS (.NET 2.1 removed)

### Version Updates

When updating to a new patch version (e.g., 8.1.2):

**Update these files:**
1. `AMIs/roles/cp/defaults/main.yml` → `cp_patch_version: '8.1.2-1'`
2. `AMIs/roles/cp-docker/defaults/main.yml` → `cp_patch_version: "8.1.2"`
3. `AMIs/cp/packer.json` → `cp_patch_version` variable
4. `AMIs/cp-docker/packer.json` → `cp_patch_version` variable
5. `AMIs/cp-docker/packer-standalone.json` → `cp_patch_version` variable

**Verify availability:**
```bash
# APT
apt-cache madison confluent-platform

# Docker
docker manifest inspect confluentinc/cp-server:8.1.2
```

### System Configuration

#### Disabled Features
- Ubuntu release upgrade prompts (stays on 22.04 LTS)
- Automatic system updates
- Chrome auto-updates and notifications
- Privacy Sandbox features
- VS Code telemetry and auto-updates

#### Enabled Features
- XRDP (Remote Desktop)
- Docker (with user `training`)
- Auto-shutdown notifications
- Custom GNOME desktop layout

### User Configuration

**Default User:** `training`
- Password: `training`
- Groups: `sudo`, `training`, `users`, `docker`
- Shell: `/bin/bash`
- Alias: `python` → `python3`

---

## Verification Commands

### Check Ubuntu Version
```bash
lsb_release -a
# Expected: Ubuntu 22.04.x LTS
```

### Check Installed Packages
```bash
# Confluent Platform
dpkg -l | grep confluent

# Java
java -version
# Expected: openjdk 21

# .NET
dotnet --version
# Expected: 8.0.x

# Docker
docker --version
docker compose version
# Expected: Docker Compose v2.24.0

# Python
python --version
# Expected: Python 3.10.x
```

### Check Docker Images
```bash
docker images | grep confluentinc
# Should list all 8.1.1 and Control Center 2.3.0 images
```

### Check System Configuration
```bash
# Chrome policies
cat /etc/opt/chrome/policies/managed/policies.json

# Ubuntu upgrade setting
grep Prompt /etc/update-manager/release-upgrades
# Expected: Prompt=never

# Docker Compose version
docker compose version
# Expected: v2.24.0 or higher (API 1.44+)
```

---

## Related Documentation

- **[README.md](README.md)** - AMI overview and build instructions
- **[COMPATIBILITY.md](COMPATIBILITY.md)** - Component compatibility matrix
- **[DOCKER_VERSIONS.md](DOCKER_VERSIONS.md)** - Docker image details
- **[VERSION_SUMMARY.md](VERSION_SUMMARY.md)** - Quick version reference
- **[TROUBLESHOOTING.md](TROUBLESHOOTING.md)** - Common build issues
- **[ORGANIZATION.md](ORGANIZATION.md)** - Repository structure

---

## References

- [Ubuntu 22.04 LTS Release Notes](https://releases.ubuntu.com/22.04/)
- [Confluent Platform 8.1 Documentation](https://docs.confluent.io/platform/8.1/overview.html)
- [Confluent APT Repository](https://packages.confluent.io/deb/)
- [Confluent Docker Hub](https://hub.docker.com/u/confluentinc)
- [OpenJDK 21 LTS](https://openjdk.org/projects/jdk/21/)
- [.NET 8.0 LTS](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)
- [Docker Compose Release Notes](https://github.com/docker/compose/releases)

