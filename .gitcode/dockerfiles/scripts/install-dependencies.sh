#!/bin/bash
set -euox pipefail
readonly BUILD_PARALLELISM="${BUILD_PARALLELISM:-16}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
source "${SCRIPT_DIR}/install-buildtools.sh"

readonly -a supported_tools=(
    bisheng-jdk-17.0.18
    bisheng-jdk-8u342
    bisheng-jdk-8u342-x86
    cmake-3.28.2
    git-2.39.3
    lcov
    maven-3.9.9
    python-3.11
)


readonly -a supported_install=(
    "${supported_tools[@]}"
)
readonly -a supported_setup=(maven)

readonly -a base_pkgs=(
    autoconf boost-devel cmake curl dos2unix file findutils gcc gcc-c++ gdb git
    glibc-locale-source iproute iputils make maven net-tools ninja-build ccache
    openssh-clients openssh-server openssl passwd patch perf sudo unzip vim wget
    xz zip zlib pcre2 rpm-build libasan libasan-static jq python3-pip
    double-conversion-devel libevent-devel glog-devel krb5-devel libffi-devel python3-devel
    binutils binutils-devel tree doxygen patchelf texinfo automake libtool openssl-devel google-benchmark-devel
)

usage() {
    cat <<'EOF'
Usage:
  bash scripts/install-dependencies.sh --install <component>
  bash scripts/install-dependencies.sh --install-base-pkgs
  bash scripts/install-dependencies.sh --install-all-tools
  bash scripts/install-dependencies.sh --setup <type>
  bash scripts/install-dependencies.sh --help

Options:
  --install <component>              Install one component
  --install-base-pkgs               Install the base OS packages required to build
  --install-all-tools               Install all build tools (jdk/cmake/maven/llvm)
  --setup <type>                    Apply one configuration type
  -h, --help                        Show this help message

Supported build‑tool targets:
EOF
    printf '  %s\n' "${supported_tools[@]}"
    printf '\nSupported setup targets:\n'
    printf '  %s\n' "${supported_setup[@]}"
}

is_supported() {
    local target="$1"
    shift
    local supported
    for supported in "$@"; do
        if [[ "$target" == "$supported" ]]; then
            return 0
        fi
    done
    return 1
}

install_base_pkgs() {
    echo -e "\n===== Install base packages ====="
    yum install -y --setopt=install_weak_deps=False "${base_pkgs[@]}"
    yum clean all
    rm -rf /var/cache/dnf /var/cache/yum
}

install_all_tools() {
    for comp in "${supported_tools[@]}"; do
        echo -e "\n===== Install tool: ${comp} ====="
        "install_${comp//[.-]/_}"
    done
}


main() {
    if [[ $# -eq 1 && ( "$1" == "-h" || "$1" == "--help" ) ]]; then
        usage
        return 0
    fi
    if [[ $# -eq 1 ]]; then
        case "$1" in
            --install-base-pkgs)
                install_base_pkgs
                return 0
                ;;
            --install-all-tools)
                install_all_tools
                return 0
                ;;
        esac
    fi
    if [[ $# -ne 2 ]]; then
        echo "Expected --install <component> / --install-base-pkgs / --install-all-tools / --setup <type>." >&2
        usage >&2
        return 1
    fi
    local opt="$1"
    local target="$2"
    case "${opt}" in
        --install)
            if ! is_supported "$target" "${supported_install[@]}"; then
                echo "Unsupported install target: $target" >&2
                usage >&2
                return 1
            fi
            "install_${target//[.-]/_}"
            ;;
        --setup)
            if ! is_supported "$target" "${supported_setup[@]}"; then
                echo "Unsupported setup type: $target" >&2
                usage >&2
                return 1
            fi
            "setup_${target//[.-]/_}"
            ;;
        *)
            echo "Unknown option: $opt" >&2
            usage >&2
            return 1
            ;;
    esac
}

main "$@"
