import { redirect } from "next/navigation";

export default function Index(): never {
  redirect("/dashboard");
}
