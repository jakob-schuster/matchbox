use crate::surface::Tm;

pub fn ids_tm(tm: &Tm) -> Vec<String> {
    match &tm.data {
        crate::surface::TmData::BoolLit { b } => vec![],
        crate::surface::TmData::NumLit { n } => vec![],
        crate::surface::TmData::StrLit { regs } => regs
            .iter()
            .flat_map(|reg| match &reg.data {
                crate::surface::StrLitRegionData::Str { s } => vec![],
                crate::surface::StrLitRegionData::Tm { tm } => ids_tm(&tm),
            })
            .collect(),
        crate::surface::TmData::RecTy { fields } => fields
            .iter()
            .flat_map(|field| ids_tm(&field.data))
            .collect(),
        crate::surface::TmData::RecWithTy { fields } => fields
            .iter()
            .flat_map(|field| ids_tm(&field.data))
            .collect(),
        crate::surface::TmData::RecLit { fields } => fields
            .iter()
            .flat_map(|field| ids_tm(&field.data))
            .collect(),
        crate::surface::TmData::RecProj { tm, name } => ids_tm(&tm),
        crate::surface::TmData::FunTy { args, body } => args
            .iter()
            .flat_map(|arg| ids_tm(arg))
            .chain(ids_tm(&body))
            .collect(),
        crate::surface::TmData::FunLit { args, body } => args
            .iter()
            .flat_map(|arg| ids_tm(&arg.ty))
            .chain(ids_tm(&body))
            .collect(),
        crate::surface::TmData::FunLitForeign { args, ty, name } => args
            .iter()
            .flat_map(|arg| ids_tm(&arg.ty))
            .chain(ids_tm(&ty))
            .collect(),
        crate::surface::TmData::FunApp { head, args } => ids_tm(&head)
            .into_iter()
            .chain(args.iter().flat_map(|arg| ids_tm(arg)).collect::<Vec<_>>())
            .collect(),
        crate::surface::TmData::BinOp { tm0, tm1, op } => {
            ids_tm(&tm0).into_iter().chain(ids_tm(&tm1)).collect()
        }
        crate::surface::TmData::UnOp { tm, op } => ids_tm(&tm),
        crate::surface::TmData::Name { name } => vec![name.clone()],
        crate::surface::TmData::ListTy { tm } => ids_tm(tm),
        crate::surface::TmData::ListLit { tms } => tms.iter().flat_map(|tm| ids_tm(&tm)).collect(),
    }
}
