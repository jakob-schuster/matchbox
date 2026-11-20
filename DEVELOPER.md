# *matchbox* contributor documentation

This is a readme file for contributors to *matchbox*. Mostly for my own reference, so I remember the processes I use.

## New *matchbox* release checklist

- Merge changes from `develop` into `main`. Make sure that you've bumped the version number in `Cargo.toml`. Check that the tests pass.
- Create a new release with changelog. Check that the Docker image completes.
- Publish the new version to `crates.io`.

## Branch strategy

*matchbox* uses the [Feature Branch](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow) strategy. 

- When you make any change, do it in a new branch called `fix/*` or `feat/*`
- Then, merge into `develop` once the work from the branch is completed
- Merge into `main` only when you are ready to version bump, publish a new release, and update the crate on `crates.io`

## Docker image

A *matchbox* docker image is built on each release, using a GitHub Action. 

The Dockerfile uses [cargo-chef](https://github.com/LukeMathWalker/cargo-chef) to create smaller images.

### Local testing

If you have docker installed, you can test the image locally. Do this if you're changing the Dockerfile at all.

```bash
# build the image
docker build -t matchbox_local .
# run the image
docker run --rm -it matchbox_local bash
```

## Testing

*matchbox* runs tests on each push. These are all snapshot tests of elaboration and evaluation, using [insta](https://github.com/mitsuhiko/insta), based on [brendanzab](https://github.com/brendanzab/)'s advice.

### Local testing

If you have insta installed via cargo, you can run the tests locally:

```bash
cargo insta test
cargo insta review
```

## Publishing to `crates.io`

After merging `develop` into `main` and making the release, you need to publish the new version of the crate.

```bash
# always dry run it first
cargo publish --dry-run
```