use indicatif::{ProgressBar, ProgressDrawTarget};
use ratatui::{
    layout::{Constraint, Layout},
    DefaultTerminal, Frame, Terminal,
};

use crate::output::OutputHandlerSummary;

pub struct Interface {
    terminal: DefaultTerminal,
}

pub struct RenderState {
    output_handler_summary: OutputHandlerSummary,
}

impl Interface {
    pub fn new(bar: ProgressBar) -> Interface {
        color_eyre::install().unwrap();
        let terminal = ratatui::init();

        // bar.set_draw_target(ProgressDrawTarget::term_like(terminal));

        Interface { terminal }
    }

    pub fn update(&mut self, output_handler_summary: &OutputHandlerSummary) {
        self.render(&RenderState {
            output_handler_summary: output_handler_summary.clone(),
        });
    }

    fn render(&mut self, state: &RenderState) {
        self.terminal.draw(|frame| render(frame, state));
    }

    pub fn finish(&self) {
        ratatui::restore();
    }
}

fn render(frame: &mut Frame, state: &RenderState) {
    let vertical = Layout::vertical([
        Constraint::Length(2),
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(15),
    ]);

    let [area_0, area_1, area_2, area_3] = vertical.areas(frame.area());

    // frame.render_widget(
    //     format!(
    //         "average {}",
    //         state.output_handler_summary.average_handler.mean
    //     ),
    //     area_0,
    // );
}
