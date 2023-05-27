# Etcd Operator

The Etcd Cluster Operator is a Kubernetes operator for managing Etcd clusters. It provides a simple and efficient way to deploy, scale, and manage Etcd clusters within a Kubernetes environment.

## Getting Started

These instructions will guide you through the process of setting up the Etcd Cluster Operator on your local machine for development and testing purposes.

### Prerequisites

-   Go 1.16+
-   Docker
-   Kind (for local testing)
-   Kustomize
-   Controller-gen

### Building

To build the operator, run:

bashCopy code

`make docker-build`

This will build the Docker image for the operator.

### Running Tests

To run the unit tests, execute:

bashCopy code

`make test`

For end-to-end tests, use:

bashCopy code

`make e2e-test`

### Deployment

To deploy the operator in a Kubernetes cluster, run:

bashCopy code

`make deploy`

This will use Kustomize to customize the operator's configuration for your specific environment and then deploy it using kubectl.

### Releasing

To create a tagged release:

bashCopy code

`make release VERSION=<version>`

Replace `<version>` with the desired version tag. This will build the Docker image, push it to the Docker repository, and create a Git tag for the release.