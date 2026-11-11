use codespan_reporting::term;
use output::OutputError;

use crate::core::EvalError;

use crate::surface::elab::elab_error::ElabError;
use crate::{global_config, input, output, parse, util};

use parse::ParseError;

use input::InputError;

use crate::input::ExecError;

use codespan_reporting::diagnostic::{Diagnostic, Label};

use codespan_reporting::files::SimpleFile;

use codespan_reporting::term::termcolor::StandardStream;

use std::process::exit;

use util::location::Location;

/// A simple wrapper around the various error types,
/// so that they can all be universally printed with codespan.
#[derive(Debug)]
pub(crate) struct GenericError {
    pub(crate) location: Option<Location>,
    pub(crate) message: String,
}

impl GenericError {
    /// Pretty prints the error with codespan, and terminates execution
    pub(crate) fn codespan_print_and_exit(&self, global_config: &global_config::GlobalConfig) {
        self.codespan_print(global_config);
        exit(1)
    }

    /// Pretty prints the error with codespan
    pub(crate) fn codespan_print(&self, global_config: &global_config::GlobalConfig) {
        let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
        let config = codespan_reporting::term::Config::default();

        let file = SimpleFile::new(
            format!("<{}>", global_config.input_code.name()),
            global_config.input_code.code().unwrap(),
        );

        // attach a location if one exists
        let diagnostic = match self.location.clone() {
            Some(location) => Diagnostic::error()
                .with_message(self.message.clone())
                .with_labels(vec![Label::primary((), location.start..location.end)]),
            None => Diagnostic::error().with_message(&self.message),
        };

        term::emit_to_io_write(&mut writer, &config, &file, &diagnostic);
    }
}

impl From<ExecError> for GenericError {
    fn from(value: ExecError) -> Self {
        match value {
            ExecError::Eval(eval_error) => GenericError::from(eval_error),
            ExecError::Input(input_error) => GenericError::from(input_error),
            ExecError::Output(output_error) => GenericError::from(output_error),
        }
    }
}

impl From<InputError> for GenericError {
    fn from(value: InputError) -> Self {
        GenericError {
            location: None,
            message: value.to_string(),
        }
    }
}

impl From<ParseError> for GenericError {
    fn from(value: ParseError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

impl From<ElabError> for GenericError {
    fn from(value: ElabError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

impl From<EvalError> for GenericError {
    fn from(value: EvalError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

impl From<OutputError> for GenericError {
    fn from(value: OutputError) -> Self {
        GenericError {
            location: None,
            message: value.to_string(),
        }
    }
}
