#include "printing_pass.hpp"
#include "runtime.hpp"
#include "pass.hpp"

PrintingPass::PrintingPass(bool direct_stdout)
 : direct_stdout(direct_stdout)
{
	flat = true;
}

void
PrintingPass::on_program(Program& p)
{
	gen_ident();
	out << "program {" << std::endl;

	scope([&] () { recurse_program(p); });

	gen_ident();
	out << "} // program" << std::endl;

	ASSERT(indent_level == 0);

	if (direct_stdout) {
		std::cout << str() << std::endl;
	}
}

void
PrintingPass::on_pipeline(Pipeline& p)
{
	gen_ident();
	out << "pipeline {" << std::endl;

	scope([&] () { recurse_pipeline(p); });

	gen_ident();
	out << "} // pipeline" << std::endl;
}

void
PrintingPass::on_lolepop(Pipeline& p, Lolepop& l)
{
	// printf("LOLEPOP %s\n", l.name.c_str());
	gen_ident();
	out << "lolepop '" << l.name << "' {" << std::endl;

	scope([&] () { recurse_lolepop(p, l); });

	gen_ident();
	out << "} // lolepop '" << l.name << "'" << std::endl;
}

void
PrintingPass::on_statement(LolepopCtx& ctx, StmtPtr& stmt)
{
	auto s = *stmt;
	auto gen = [&] (int pos) {
		if (pos >= 0) {
			return;
		}

		gen_ident();
		switch (s.type) {
		case Statement::Type::Loop:
			out << "loop ";
			break;
		case Statement::Type::Assignment:
			out << s.var << " = ";
			break;

		case Statement::Type::EffectExpr:
			out << "effect ";
			break;

		case Statement::Type::Emit:
			out << "emit ";
			break;
		case Statement::Type::Done:
			out << "done ";
			break;
		case Statement::Type::Wrap:
			out << "wrap ";
			break;
		case Statement::Type::BlendStmt:
			out << "blend_stmt ";
			break;
		case Statement::Type::MetaStmt:
			out << "meta ";
			{
				auto meta = (MetaStmt* )(stmt.get());
				switch (meta->meta_type) {
				case MetaStmt::MetaType::VarDead:
					{
						auto dead = (MetaVarDead*)meta;
						out << "dead '" << dead->variable_name << "'";
					}
					break;
				default:
					out << "???";
					break;
				}
			}
			break;
		default:
			ASSERT(false);
			break;
		}

		out << std::endl;
	};

	gen(-1);
	scope([&] () { recurse_statement(ctx, stmt, true); });

	gen(0);
	scope([&] () { recurse_statement(ctx, stmt, false); });

	gen(1);

	gen_ident();
	
}

void
PrintingPass::on_expression(LolepopCtx& ctx, ExprPtr& expr)
{
	auto s = *expr;
	// printf("EXPR %s\n", s.fun.c_str());
	gen_ident();
	switch (s.type) {
	case Expression::Type::Function:
		out << "!" << s.fun;
		break;
	case Expression::Type::Constant:
		out << "\"" << s.fun << "\"";
		break;
	case Expression::Type::Reference:
		out << "@" << s.fun;
		break;
	case Expression::Type::LoleArg:
		out << "LoleArg";
		break;
	case Expression::Type::LolePred:
		out << "LolePred";
		break;
	default:
		ASSERT(false);
		break;
	}

	out << "\t\t";

	if (s.props.constant) {
		out << "constant ";
	}
	if (s.props.scalar) {
		out << "scalar ";
	}
	
	if (s.props.type.arity.size() > 1) {
		out << "type { ";
		for (size_t i=0; i<s.props.type.arity.size(); i++) {
			auto& t = s.props.type.arity[i];
			if (i > 0) {
				out << ", ";
			}
			if (i > 4) {
				out << "...";
				break;
			}
			out << t.type;
		}
		out << " }";
	} else if (s.props.type.arity.size() > 0) {
		out << "type { ";
		auto& t = s.props.type.arity[0];
		out << t.type << "[" << t.dmin << ", " << t.dmax << "]";
		out << " }";
	}
	
	out << std::endl;
	scope([&] () { recurse_expression(ctx, expr); });
}

void
PrintingPass::on_data_structure(DataStructure& d)
{
	const auto& name = d.name;
	const auto type = DataStructure::type_to_str(d.type);

	gen_ident();
	out << type << " '" << name << "'" << " {" << std::endl;

	scope([&] () {
		for (auto& col : d.cols) {
			gen_ident();
			std::string t;
			switch (col.mod) {
			case DCol::Modifier::kValue:
				t = "value";
				break;
			case DCol::Modifier::kKey:
				t = "key";
				break;
			case DCol::Modifier::kHash:
				t = "hash";
				break;
			default:
				ASSERT(false);
				break;
			}
			out << t << " { '" << col.name << "', source='" << col.source << "' }" << std::endl;
		}
	});

	gen_ident();
	out << "} // " << type << " '" << name << "'" << std::endl;
}

void
PrintingPass::gen_ident(std::ostringstream& out)
{
	size_t num = indent_level * indent_spaces;
	for (size_t i=0; i<num; i++) {
		out << indent_string;
	}
}

void
PrintingPass::gen_ident()
{
	return gen_ident(out);
}

std::string
PrintingPass::str()
{
	return out.str();
}
