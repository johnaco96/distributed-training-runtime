#![forbid(unsafe_code)]

//! Prints the TrainingRun CRD as YAML.
//!
//! Regenerate the committed manifest after editing `crd.rs`:
//!
//! ```bash
//! cargo run -p runtime-operator --bin crdgen > k8s/operator/crd-trainingrun.yaml
//! ```

use kube::CustomResourceExt;

fn main() {
    print!(
        "{}",
        serde_yaml::to_string(&runtime_operator::crd::TrainingRun::crd())
            .expect("CRD serialises to YAML")
    );
}
